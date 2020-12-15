#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <memory>
#include <functional>
#include <emmintrin.h>
#include <cstring>
#include <iostream>

#include <bitset>

#include "flow/flow.h"

#define MAX_PREFIX_LEN 8

#define NODE4 1
#define NODE16 2
#define NODE48 3
#define NODE256 4
#define LEAF 5
#define VARNODE 6

#define PART_MAX_KEY_LEN 10000


bool debug = false;

#ifdef USE_VARNODE
static int grow_varnode(int capacity) {
	int new_capacity = capacity * 1.1;
	if (new_capacity == capacity)
		return capacity + 1;
	else
		return new_capacity;
}
#endif

static inline int min(int a, int b) {
	return (a < b) ? a : b;
}

template <typename V>
class Leaf;

#ifdef USE_VARNODE
template <typename V>
class ArtVarNode;
#endif

template <typename V>
class Node {
public:
	Node(uint8_t type) : refcount(0), type(type), checkpointed(false) {}
	Node(const Node<V>& other) : refcount(0), type(other.type), checkpointed(other.checkpointed) {}

	int refcount;
	uint8_t type;
	bool checkpointed;

	static Node<V>* clone(const Node<V>* n);
	static Node<V>* reorder_leaves(Node<V>* n);
	static Leaf<V>* minimum(Node<V>* n);
	static Leaf<V>* insert(Node<V>* n, Node<V>** ref, const unsigned char* key, int key_len, V value, int depth,
                         bool force_clone);
	static void iter(Node<V>* n, std::function<void(const unsigned char*, uint32_t, V)> cb);
	static int decrement_refcount(Node<V>* n);
	static void iter_nodes(Node<V>* n, std::function<bool(Node*, int)> cb, int depth);
	static int node_size(Node<V>* n);

	static void switch_ref(Node<V>** ref, Node<V>* n) {
		// First increment the refcount of the new node, in case it would
		// otherwise have been deleted by the decrement of the old node
		n->refcount++;
		if (*ref) {
			Node<V>::decrement_refcount(*ref);
		}
		*ref = n;
	}

	static void switch_ref_no_decrement(Node<V>** ref, Node<V>* n) {
		n->refcount++;
		*ref = n;
	}
};

template <typename V>
class Leaf : public Node<V>, FastAllocated<Leaf<V>> {
public:
	static int count;

	static Leaf<V>* create(const unsigned char* key, int key_len, V value) { return new Leaf<V>(key, key_len, value); }
	static Leaf<V>* create(const Leaf<V>& other) { return new Leaf<V>(other); }

private:
	Leaf(const unsigned char* key, int key_len, V value) : Node<V>(LEAF), value(value), key_len(key_len) {
		this->key = (unsigned char*)malloc(key_len);
		memcpy(this->key, key, key_len);
		count++;
	}

	Leaf(const Leaf<V>& other) : Node<V>(other), value(other.value), key_len(other.key_len) {
		memcpy(this->key, other.key, key_len);
		count++;
	}

public:
	/** Returns 0 if key matches */
	int matches(const unsigned char* key, int key_len) {
		// Fail if the key lengths are different
		if (this->key_len != (uint32_t)key_len) return 1;

		// Compare the keys
		return memcmp(this->key, key, key_len);
	}

	Leaf<V>* minimum() { return this; }

	int longest_common_prefix(Leaf<V>* other, int depth) {
		int max_cmp = min(key_len, other->key_len) - depth;
		int idx;
		for (idx = 0; idx < max_cmp; idx++) {
			if (key[depth + idx] != other->key[depth + idx]) {
				return idx;
			}
		}
		return idx;
	}

	Leaf<V>* insert(Node<V>** ref, const unsigned char* key, int key_len, V value, int depth, bool force_clone);

	void iter(std::function<void(const unsigned char*, uint32_t, V)> cb) { cb(key, key_len, value); }

	void iter_nodes(std::function<bool(Node<V>*, int)> cb, int depth) { cb(this, depth); }

	Leaf<V>* reorder_leaves() {
		auto new_leaf = Leaf<V>::create(key, key_len, value);
		new_leaf->refcount++;
		return new_leaf;
		// return this;
	}

	int decrement_refcount() {
		if (--this->refcount <= 0) {
      std::cout << "fucker\n";
			count--;
			// delete[] key;
			// delete[] value;
			free(key);
			delete this;
			return sizeof(Leaf<V>);
		}
		return 0;
	}

	V value;
	uint32_t key_len;
	unsigned char* key;
};

template <typename V>
class ArtNode : public Node<V> {
public:
	ArtNode(uint8_t type) : Node<V>(type), num_children(0), partial_len(0) {}

	ArtNode(const ArtNode<V>& other)
	  : Node<V>(other), num_children(other.num_children), partial_len(other.partial_len) {
		memcpy(partial, other.partial, min(MAX_PREFIX_LEN, partial_len));
	}

	/**
	 * Returns the number of prefix characters shared between
	 * the key and node.
	 */
  // TODO: simd this
	int check_prefix(const unsigned char* key, int key_len, int depth) {
		int max_cmp = min(min(partial_len, MAX_PREFIX_LEN), key_len - depth);
		int idx;
		for (idx = 0; idx < max_cmp; idx++) {
			if (partial[idx] != key[depth + idx]) return idx;
		}
		return idx;
	}

	/**
	 * Calculates the index at which the prefixes mismatch
	 */
	int prefix_mismatch(const unsigned char* key, int key_len, int depth) {
		int max_cmp = min(min(MAX_PREFIX_LEN, partial_len), key_len - depth);
		int idx;
		for (idx = 0; idx < max_cmp; idx++) {
			if (partial[idx] != key[depth + idx]) return idx;
		}

		// If the prefix is short we can avoid finding a leaf
		if (partial_len > MAX_PREFIX_LEN) {
			// Prefix is longer than what we've checked, find a leaf
			const Leaf<V>* l = Node<V>::minimum(this);
			max_cmp = min(l->key_len, key_len) - depth;
			for (; idx < max_cmp; idx++) {
				if (l->key[idx + depth] != key[depth + idx]) return idx;
			}
		}
		return idx;
	}

	int signed_prefix_mismatch(const unsigned char* key, int key_len, int depth) {
		int max_cmp = min(min(MAX_PREFIX_LEN, partial_len), key_len - depth);
		int idx;
		for (idx = 0; idx < max_cmp; idx++) {
			if (partial[idx] != key[depth + idx]) return partial[idx] - key[depth + idx];
		}

		// If the prefix is short we can avoid finding a leaf
		if (partial_len > MAX_PREFIX_LEN) {
			// Prefix is longer than what we've checked, find a leaf
			const Leaf<V>* l = Node<V>::minimum(this);
			max_cmp = min(l->key_len, key_len) - depth;
			for (; idx < max_cmp; idx++) {
				if (l->key[idx + depth] != key[depth + idx]) return l->key[idx + depth] - key[depth + idx];
			}
		}
		return 0;
	}

	static Node<V>** find_child(ArtNode<V>* n, unsigned char c);
  // Finds the node immediately after `c`.
  static Node<V>** find_next(ArtNode<V>* n, unsigned char c);

  // The bool argument is returning whether the char was an exact match or not.
	static std::pair<Node<V>**, bool> lower_bound(ArtNode<V>* n, unsigned char c);

	static void add_child(ArtNode<V>* n, Node<V>** ref, unsigned char c, Node<V>* child);

	Leaf<V>* insert(Node<V>** ref, const unsigned char* key, int key_len, V value, int depth, bool force_clone);

	uint8_t num_children;
	uint8_t partial_len;
	unsigned char partial[MAX_PREFIX_LEN];
};

template <typename V>
class ArtNode4 : public ArtNode<V>, FastAllocated<ArtNode4<V>> {
public:
	static int count;

	static ArtNode4<V>* create() { return new ArtNode4<V>; }
	static ArtNode4<V>* create(const ArtNode4<V>& other) { return new ArtNode4<V>(other); }

private:
	ArtNode4() : ArtNode<V>(NODE4), keys() { count++; }

	ArtNode4(const ArtNode4<V>& other) : ArtNode<V>(other) {
		memcpy(keys, other.keys, this->num_children);
		for (int i = 0; i < this->num_children; i++) {
			children[i] = other.children[i];
			children[i]->refcount++;
		}
		count++;
	}

public:
  // TODO: Consider unrolling these loops.
	Node<V>** find_child(unsigned char c) {
		for (int i = 0; i < this->num_children; i++) {
			if (keys[i] == c) {
				return &children[i];
			}
		}
		return NULL;
	}

	Node<V>** find_next(unsigned char c) {
    if(debug)std::cout << "next art4\n";
		for (int i = 0; i < this->num_children; i++) {
      if(debug)std::cout << keys[i] << " ";
			if (keys[i] > c) {
        if(debug)std::cout << "\n";
				return &children[i];
			}
		}
    if(debug)std::cout << "\n";
		return NULL;
	}

  std::pair<Node<V>**, bool> lower_bound(unsigned char c) {
    if (debug) {
      std::cout << "art 4\n";
      std::cout << "char: " << c << '\n';
    }
		for (int i = 0; i < this->num_children; i++) {
      if (debug) {
        std::cout << keys[i] << ' ';
      }
			if (keys[i] >= c) {
        if(debug)std::cout << "\n";
				return {&children[i], keys[i] == c};
			}
		}
    if(debug)std::cout<<"\n";
		return {nullptr, false};
	}

	Leaf<V>* minimum() {
		if (children[0]) return Node<V>::minimum(children[0]);
		return NULL;
	}

	void add_child(Node<V>** ref, unsigned char c, Node<V>* child);

	void iter(std::function<void(const unsigned char*, uint32_t, V)> cb) {
		for (int i = 0; i < this->num_children; i++) {
			Node<V>::iter(children[i], cb);
		}
	}

	void iter_nodes(std::function<bool(Node<V>*, int)> cb, int depth) {
		if (cb(this, depth)) {
			for (int i = 0; i < this->num_children; i++) {
				Node<V>::iter_nodes(children[i], cb, depth + 1);
			}
		}
	}

	ArtNode4<V>* reorder_leaves() {
		for (int i = 0; i < this->num_children; i++) {
			children[i] = Node<V>::reorder_leaves(children[i]);
		}
		return this;
	}

	int decrement_refcount() {
		if (--this->refcount <= 0) {
			int freed = 0;
			for (int i = 0; i < this->num_children; i++) {
				freed += Node<V>::decrement_refcount(children[i]);
			}
			count--;
			delete this;
			return freed + sizeof(ArtNode4<V>);
		}
		return 0;
	}

	uint8_t keys[4];
	Node<V>* children[4];
};

// TODO: move this to a portable header.
#define _mm_cmpge_epu8(a, b) \
        _mm_cmpeq_epi8(_mm_max_epu8(a, b), a)

#define _mm_cmple_epu8(a, b) _mm_cmpge_epu8(b, a)

#define _mm_cmpgt_epu8(a, b) \
        _mm_xor_si128(_mm_cmple_epu8(a, b), _mm_set1_epi8(-1))


template <typename V>
class ArtNode16 : public ArtNode<V>, FastAllocated<ArtNode16<V>> {
public:
	static int count;

	static ArtNode16<V>* create() { return new ArtNode16<V>; }
	static ArtNode16<V>* create(const ArtNode16<V>& other) { return new ArtNode16<V>(other); }
	static ArtNode16<V>* create(ArtNode4<V>* other) { return new ArtNode16<V>(other); }

private:
	ArtNode16() : ArtNode<V>(NODE16), keys() { count++; }

	ArtNode16(const ArtNode16<V>& other) : ArtNode<V>(other) {
		memcpy(keys, other.keys, this->num_children);
		for (int i = 0; i < this->num_children; i++) {
			children[i] = other.children[i];
			children[i]->refcount++;
		}
		count++;
	}

	ArtNode16(ArtNode4<V>* other) : ArtNode16<V>() {
		// ArtNode
		this->num_children = other->num_children;
		this->partial_len = other->partial_len;
		memcpy(this->partial, other->partial, min(MAX_PREFIX_LEN, this->partial_len));
		// ArtNode16 from ArtNode4
		memcpy(keys, other->keys, this->num_children);
		for (int i = 0; i < this->num_children; i++) {
			children[i] = other->children[i];
			children[i]->refcount++;
		}
	}

public:
	Node<V>** find_child(unsigned char c) {
		// Compare the key to all 16 stored keys
		__m128i cmp = _mm_cmpeq_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i*)keys));
		// Use a mask to ignore children that don't exist
		int mask = (1 << this->num_children) - 1;
		int bitfield = _mm_movemask_epi8(cmp) & mask;

		/*
		 * If we have a match (any bit set) then we can
		 * return the pointer match using ctz to get
		 * the index.
		 */
		if (bitfield) {
			return &children[__builtin_ctz(bitfield)];
		}
		return NULL;
	}

	Node<V>** find_next(unsigned char c) {
    if(debug)std::cout << "next art16\n";
		for (int i = 0; i < this->num_children; i++) {
			if (keys[i] > c) {
				return &children[i];
			}
		}
    return nullptr;

		// Compare the key to all 16 stored keys
		__m128i cmp = _mm_cmpgt_epi8(_mm_loadu_si128((__m128i*)keys), _mm_set1_epi8(c));
		// Use a mask to ignore the last child and children that don't exist
		int mask = (1 << this->num_children) - 1;
		int bitfield = _mm_movemask_epi8(cmp) & mask;

		/*
		 * If we have a match (any bit set) then we can
		 * return the pointer match using ctz to get
		 * the index.
		 */
		if (bitfield) {
			return &children[__builtin_ctz(bitfield)];
		}
		return NULL;
	}

  std::pair<Node<V>**, bool> lower_bound(unsigned char c) {
    if (debug) {
      std::cout << "art16\n";
      std::cout << std::string(1, c) << "\n";
      std::cout << int(c) << "\n";
    }

		// Compare the key to all 16 stored keys
		__m128i cmp = _mm_cmpgt_epi8(_mm_set1_epi8(c), _mm_loadu_si128((__m128i*)keys));
		// Use a mask to ignore children that don't exist
		int mask = (1 << this->num_children) - 1;
		int bitfield = ~_mm_movemask_epi8(cmp) & mask;

    if (debug) {
      std::cout << "bits: " << std::bitset<32>(bitfield) << "\n";
      std::cout << "mask: " << std::bitset<32>(mask) << "\n";
      std::cout << "cmp: " << std::bitset<32>(_mm_movemask_epi8(cmp)) << "\n";
      for (int i = 0; i < this->num_children; ++i) {
        std::cout << int(keys[i]) << " ";
      }
      std::cout << "\n";
    }

		/*
		 * If we have a match (any bit set) then we can
		 * return the pointer match using ctz to get
		 * the index.
		 */
		if (bitfield) {
      int idx = __builtin_ctz(bitfield);
      if (debug) {
        std::cout << "index: " << idx << "\n";
      }
      if (keys[idx] < c) {
        std::cout << "char: " << int(c) << "\n";
        std::cout << "index: " << idx << "\n";
        std::cout << "bits: " << std::bitset<32>(bitfield) << "\n";
        std::cout << "mask: " << std::bitset<32>(mask) << "\n";
        for (int i = 0; i < this->num_children; ++i) {
          std::cout << int(keys[i]) << " ";
        }
        std::cout << "\n";
        ASSERT(false);
      }
      return {&children[idx], keys[idx] == c};
		}
    if (c == keys[this->num_children-1]) {
      return {&children[this->num_children-1], true};
    }
		return {nullptr, false};
	}

	Leaf<V>* minimum() {
		if (children[0]) return Node<V>::minimum(children[0]);
		return NULL;
	}

	void add_child(Node<V>** ref, unsigned char c, Node<V>* child);

	void iter(std::function<void(const unsigned char*, uint32_t, V)> cb) {
		for (int i = 0; i < this->num_children; i++) {
			Node<V>::iter(children[i], cb);
		}
	}

	void iter_nodes(std::function<bool(Node<V>*, int)> cb, int depth) {
		if (cb(this, depth)) {
			for (int i = 0; i < this->num_children; i++) {
				Node<V>::iter_nodes(children[i], cb, depth + 1);
			}
		}
	}

	ArtNode16<V>* reorder_leaves() {
		for (int i = 0; i < this->num_children; i++) {
			children[i] = Node<V>::reorder_leaves(children[i]);
		}
		return this;
	}

	int decrement_refcount() {
		if (--this->refcount <= 0) {
			int freed = 0;
			for (int i = 0; i < this->num_children; i++) {
				freed += Node<V>::decrement_refcount(children[i]);
			}
			count--;
			delete this;
			return freed + sizeof(ArtNode16<V>);
		}
		return 0;
	}

  // TODO: Consider just storing this directly as a __m128i.
	uint8_t keys[16];
	Node<V>* children[16];
};

template <typename V>
class ArtNode48 : public ArtNode<V>, FastAllocated<ArtNode48<V>> {
public:
	static int count;

	static ArtNode48<V>* create() { return new ArtNode48<V>; }
	static ArtNode48<V>* create(const ArtNode48<V>& other) { return new ArtNode48<V>(other); }
	static ArtNode48<V>* create(ArtNode16<V>* other) { return new ArtNode48<V>(other); }

private:
	ArtNode48() : ArtNode<V>(NODE48), keys() { count++; }

	ArtNode48(const ArtNode48<V>& other) : ArtNode<V>(other) {
		memcpy(keys, other.keys, 256);
		for (int i = 0; i < this->num_children; i++) {
			children[i] = other.children[i];
			children[i]->refcount++;
		}
		count++;
	}

	ArtNode48(ArtNode16<V>* other) : ArtNode48<V>() {
		// ArtNode
		this->num_children = other->num_children;
		this->partial_len = other->partial_len;
		memcpy(this->partial, other->partial, min(MAX_PREFIX_LEN, this->partial_len));

		// ArtNode48 from ArtNode16
		for (int i = 0; i < this->num_children; i++) {
			keys[other->keys[i]] = i + 1;
			children[i] = other->children[i];
			children[i]->refcount++;
		}
	}

public:
	Node<V>** find_child(unsigned char c) {
		int idx = keys[c];
		if (idx) return &children[idx - 1];
		return NULL;
	}

	Node<V>** find_next(unsigned char c) {
    if(debug)std::cout << "next art48\n";
    if (c == 255) return nullptr;
    // TODO: simd
    for (int i = c + 1; i < 256; ++i) {
      if (keys[i]) {
        return &children[keys[i] - 1];
      }
    }
		return nullptr;
	}

  std::pair<Node<V>**, bool> lower_bound(unsigned char c) {
    if (debug) {
      std::cout << "art48\n";
      std::cout << c << "\n";
    }

		int idx = keys[c];
    if (idx) {
      return {&children[idx - 1], true};
    }

    // TODO: simd
    for (int i = c + 1; i < 256; ++i) {
      if (debug) {
        std::cout << "char: " << i << "\n";
        std::cout << "key: " << int(keys[i]) << "\n";
      }
      if (keys[i]) {
        return {&children[keys[i] - 1], false};
      }
    }
    if (debug) {
      std::cout << "didn't find it\n";
    }
		return {nullptr, false};
	}

	Leaf<V>* minimum() {
		int idx = 0;
		while (!keys[idx]) idx++;
		auto child = children[keys[idx] - 1];
		if (child) return Node<V>::minimum(child);
		return NULL;
	}

	void add_child(Node<V>** ref, unsigned char c, Node<V>* child);

	void iter(std::function<void(const unsigned char*, uint32_t, V)> cb) {
		for (int i = 0; i < 256; i++) {
			int idx = keys[i];
			if (idx) {
				Node<V>::iter(children[idx - 1], cb);
			}
		}
	}

	void iter_nodes(std::function<bool(Node<V>*, int)> cb, int depth) {
		if (cb(this, depth)) {
			for (int i = 0; i < 256; i++) {
				int idx = keys[i];
				if (idx) {
					Node<V>::iter_nodes(children[idx - 1], cb, depth + 1);
				}
			}
		}
	}

	ArtNode48<V>* reorder_leaves() {
		for (int i = 0; i < 256; i++) {
			int idx = keys[i];
			if (idx) {
				children[idx - 1] = Node<V>::reorder_leaves(children[idx - 1]);
			}
		}
		return this;
	}

	int decrement_refcount() {
		if (--this->refcount == 0) {
			int freed = 0;
			for (int i = 0; i < this->num_children; i++) {
				freed += Node<V>::decrement_refcount(children[i]);
			}
			count--;
			delete this;
			return freed + sizeof(ArtNode48<V>);
		}
		return 0;
	}

	uint8_t keys[256]; // the value 0 is reserved to mean empty, so we add 1 to
	                   // all indices here
	Node<V>* children[48];
};

template <typename V>
class ArtNode256 : public ArtNode<V>, FastAllocated<ArtNode256<V>> {
public:
	static int count;

	static ArtNode256<V>* create() { return new ArtNode256<V>; }
	static ArtNode256<V>* create(const ArtNode256<V>& other) { return new ArtNode256<V>(other); }
	static ArtNode256<V>* create(ArtNode48<V>* other) { return new ArtNode256<V>(other); }

private:
	ArtNode256() : ArtNode<V>(NODE256), children() { count++; }

	ArtNode256(const ArtNode256<V>& other) : ArtNode<V>(other) {
		for (int i = 0; i < 256; i++) {
			children[i] = other.children[i];
			if (children[i]) {
				children[i]->refcount++;
			}
		}
		count++;
	}

	ArtNode256(ArtNode48<V>* other) : ArtNode256<V>() {
		// ArtNode
		this->num_children = other->num_children;
		this->partial_len = other->partial_len;
		memcpy(this->partial, other->partial, min(MAX_PREFIX_LEN, this->partial_len));
		// ArtNode256 from ArtNode48
		for (int i = 0; i < 256; i++) {
			if (other->keys[i]) {
				children[i] = other->children[other->keys[i] - 1];
				children[i]->refcount++;
			}
		}
	}

#ifdef USE_VARNODE
	ArtNode256(ArtVarNode<V>* other);
#endif

public:
	Node<V>** find_child(unsigned char c) {
		if (children[c]) return &children[c];
		return NULL;
	}

	Node<V>** find_next(unsigned char c) {
    if (debug) {
      std::cout << "next art256\n";}
    if (c == 255) return nullptr;
    // TODO: simd
    for (int i = c + 1; i < 256; ++i) {
      if(debug)std::cout << (char)i << " ";
      if (children[i]) {
        if(debug)std::cout << "\n";
        return &children[i];
      }
    }
    if(debug)std::cout << "\n";
		return nullptr;
	}

  std::pair<Node<V>**, bool> lower_bound(unsigned char c) {
    if (debug) {
    std::cout << "art256\n";
    std::cout << std::string(1, c) << "\n";
    }

		if (children[c]) return {&children[c], true};
    // TODO: simd
    for (int i = c + 1; i < 256; ++i) {
      if (children[i]) {
        return {&children[i], false};
      }
    }

    if (debug) {
      std::cout << "didn't find it\n";
    }
		return {nullptr, false};
	}

	Leaf<V>* minimum() {
		int idx = 0;
		while (!children[idx]) idx++;
		if (children[idx]) return Node<V>::minimum(children[idx]);
		return NULL;
	}

	void add_child(Node<V>** ref, unsigned char c, Node<V>* child) {
		(void)ref;
		this->num_children++;
		this->children[c] = child;
		child->refcount++;
	}

	void iter(std::function<void(const unsigned char*, uint32_t, V)> cb) {
		for (int i = 0; i < 256; i++) {
			if (children[i]) {
				Node<V>::iter(children[i], cb);
			}
		}
	}

	void iter_nodes(std::function<bool(Node<V>*, int)> cb, int depth) {
		if (cb(this, depth)) {
			for (int i = 0; i < 256; i++) {
				if (children[i]) {
					Node<V>::iter_nodes(children[i], cb, depth + 1);
				}
			}
		}
	}

	ArtNode256<V>* reorder_leaves() {
		for (int i = 0; i < 256; i++) {
			if (children[i]) {
				children[i] = Node<V>::reorder_leaves(children[i]);
			}
		}
		return this;
	}

	int decrement_refcount() {
		if (--this->refcount == 0) {
			int freed = 0;
			for (int i = 0; i < 256; i++) {
				if (children[i]) {
					freed += Node<V>::decrement_refcount(children[i]);
				}
			}
			count--;
			delete this;
			return freed + sizeof(ArtNode256<V>);
		}
		return 0;
	}

private:
	Node<V>* children[256];
};

#ifdef USE_VARNODE
template <typename V>
class ArtVarNode : public ArtNode<V>, FastAllocated<ArtVarNode<V>> {
public:
	static int count;

	ArtVarNode(uint16_t capacity) : ArtNode<V>(VARNODE), capacity(capacity), keys() { count++; }

	ArtVarNode(uint8_t capacity, const ArtVarNode<V>& other) : ArtNode<V>(other), capacity(capacity) {
		memcpy(keys, other.keys, 256);
		for (int i = 0; i < this->num_children; i++) {
			children[i] = other.children[i];
			children[i]->refcount++;
		}
		count++;
	}

	ArtVarNode(ArtNode16<V>* other) : ArtVarNode<V>(32) {
		// ArtNode
		this->num_children = other->num_children;
		this->partial_len = other->partial_len;
		memcpy(this->partial, other->partial, min(MAX_PREFIX_LEN, this->partial_len));

		// ArtVarNode from ArtNode16
		for (int i = 0; i < this->num_children; i++) {
			keys[other->keys[i]] = i + 1;
			children[i] = other->children[i];
			children[i]->refcount++;
		}
	}

	Node<V>** find_child(unsigned char c) {
		int idx = keys[c];
		if (idx) return &children[idx - 1];
		return NULL;
	}

  Leaf<V>* minimum() {
		int idx = 0;
		while (!keys[idx]) idx++;
		auto child = children[keys[idx] - 1];
		if (child) return Node<V>::minimum(child);
		return NULL;
	}

	void add_child(Node<V>** ref, unsigned char c, Node<V>* child);

	void iter(std::function<void(const unsigned char*, uint32_t, V)> cb) {
		for (int i = 0; i < 256; i++) {
			int idx = keys[i];
			if (idx) {
				Node<V>::iter(children[idx - 1], cb);
			}
		}
	}

	int decrement_refcount() {
		if (--this->refcount == 0) {
			int freed = 0;
			for (int i = 0; i < this->num_children; i++) {
				freed += Node<V>::decrement_refcount(children[i]);
			}
			count--;
			size_t this_size = offsetof(class ArtVarNode<V>, children[this->capacity]);
			free(this);
			return freed + this_size;
		}
		return 0;
	}

	uint8_t capacity;
	uint8_t keys[256]; // the value 0 is reserved to mean empty, so we add 1 to
	                   // all indices here
	Node<V>* children[1];
};
#endif

template<class V>
KeyRef MakeKeyRef(Leaf<V>* l) {
  return KeyRef(l->key, l->key_len);
}

template <typename V>
class ArtTree {
public:
	ArtTree() : root(NULL) {}

	ArtTree(const ArtTree<V>& other) { root = other.root; }

	ArtTree snapshot() {
		ArtTree<V> b;
		if (root) {
			b.root = Node<V>::clone(root);
			b.root->refcount++;
		}
		return b;
	}

	ArtTree reorder_leaves() {
		ArtTree<V> b;
		if (root) {
			b.root = Node<V>::reorder_leaves(root);
		}
		return b;
	}

  enum class BoundCheck {
    FOUND = 0,
    DEPTH = 1,
    BACKTRACK = 2,
  };

  BoundCheck check_bound_node(Node<V> *n, const unsigned char* key, int key_len, int *depth_p, Leaf<V> **result, bool strict) {
    if (n->type == LEAF) {
      auto* l = static_cast<Leaf<V>*>(n);
      int len = std::min(key_len, (int)l->key_len);
      // TODO: Take advantage of `depth`.
      int cmp = std::memcmp(l->key, key, len);
      if (debug) {
        std::cout << "leafy key: " << std::string((const char*)l->key, l->key_len) << "\n";
        std::cout << "cmp: " << cmp << "\n";
      }
      ASSERT(l);
      if (cmp < 0 || (cmp == 0 && strict)) {
        if (debug) std::cout << "We must backtrack on the leaf!";
        return BoundCheck::BACKTRACK;
      }
      *result = l;
      return BoundCheck::FOUND;
    }

    auto* an = static_cast<ArtNode<V>*>(n);

    int depth = *depth_p;
    // Case 1: This node already has a longer key than our key.
    if (key_len <= (depth + an->partial_len)) {
      if (!an->partial_len) {
        *result = ArtNode<V>::minimum(an);
        return BoundCheck::FOUND;
      }
      int prefix_differs = an->signed_prefix_mismatch(key, key_len, depth);
      // The search key is greater than the smallest key in this subtree. Backtrack.
      if (prefix_differs < 0) {
        return BoundCheck::BACKTRACK;
      } else {
        // Our key is less than or equal to the smallest key in this subtree, so return it.
        // TODO: This sometimes duplicates work with `signed_check_prefix`.
        *result = ArtNode<V>::minimum(an);
        return BoundCheck::FOUND;
      }
    }
    // Case 2: We need to keep going down the tree.
    if (!an->partial_len) {
      return BoundCheck::DEPTH;
    }
    int prefix_differs = an->signed_prefix_mismatch(key, key_len, depth);
    if (prefix_differs == 0) {
      // Any mismatch happens after the prefix covered by the node, so it is fine to check its children
      *depth_p += an->partial_len;
      return BoundCheck::DEPTH;
    }
    // A mismatch happens in the prefix of this node. Now there are two options
    // The subtree stores smaller keys. Have to backtrack
    if (prefix_differs < 0) {
      return BoundCheck::BACKTRACK;
    }
    *result = ArtNode<V>::minimum(an);
    return BoundCheck::FOUND;
  }

  struct stack_entry {
    ArtNode<V> *node;
    unsigned char key;
  };

  struct iterator {
    Leaf<V>* leaf = nullptr;
    std::vector<stack_entry> stack;

    // TODO: remove this and replace with `end()`.
    operator bool() const { return leaf != nullptr; }
    KeyRef operator*() const { return KeyRef(leaf->key, leaf->key_len); }

    bool operator==(const iterator& o) {
      return leaf == o.leaf;
    }
  };

  iterator bound_iterative(Node<V>* n, const unsigned char* key, int key_len, bool strict) {
    iterator iter;
    iter.stack.reserve(10);
    ASSERT(!iter.leaf);

    int depth = 0;
    while(n) {
      auto check = check_bound_node(n, key, key_len, &depth, &iter.leaf, strict);
      if (check == BoundCheck::FOUND) {
        return iter;
      }

      if (check == BoundCheck::BACKTRACK) break;
      // If `n` were a leaf node, we would have already returned it.
      auto* an = static_cast<ArtNode<V>*>(n);

      // Keep going down the tree.
      // TODO: This is probably done too early.
      iter.stack.push_back(stack_entry{an, key[depth]});

      auto [child, matches] = ArtNode<V>::lower_bound(an, key[depth]);
      // Time to backtrack.
      if (!child) break;
      // If the character isn't an exact match, every key under this node is greater than
      // the input key. We just need to grab the smallest one.
      if (!matches && child) {
        iter.leaf = ArtNode<V>::minimum(*child);
        return iter;
      }
      n = *child;
      depth = depth + 1;
    }

    // Handle backtracking.
    if (debug) {
      std::cout << "backtracking\n";
    }
    while (!iter.stack.empty()) {
      auto entry = iter.stack.back();
      iter.stack.pop_back();
      if (debug) {
        std::cout << "char: " << entry.key << "\n";
      }
      auto* an = ArtNode<V>::find_next(entry.node, entry.key);
      if (an) {
        iter.leaf = ArtNode<V>::minimum(*an);
        return iter;
      }
    }
    return iter;
  }

  iterator lower_bound(const unsigned char* key, int key_len) {
    if (!root) return {};
    return bound_iterative(root, key, key_len, false);
  }

  iterator upper_bound(const unsigned char* key, int key_len) {
    if (!root) return {};
    return bound_iterative(root, key, key_len, true);
  }

	Leaf<V>* search(const unsigned char* key, int key_len) {
		Node<V>* n = root;
		int prefix_len, depth = 0;
		while (n) {
			// Might be a leaf
			if (n->type == LEAF) {
				Leaf<V>* l = static_cast<Leaf<V>*>(n);
				// Check if the expanded path matches
				if (!l->matches(key, key_len)) {
					return l;
				} else {
					return NULL;
				}
			} else {
				ArtNode<V>* an = static_cast<ArtNode<V>*>(n);

				// Bail if the prefix does not match
				if (an->partial_len) {
					prefix_len = an->check_prefix(key, key_len, depth);
					if (prefix_len != min(MAX_PREFIX_LEN, an->partial_len)) {
						return NULL;
					}
					depth += an->partial_len;
				}

				// Recursively search
				Node<V>** child = ArtNode<V>::find_child(an, key[depth]);
				n = child ? *child : NULL;
				depth++;
			}
		}
		return NULL;
	}

	iterator insert(const unsigned char* key, int key_len, V value) {
    iterator iter;
		iter.leaf = Node<V>::insert(root, &root, key, key_len, value, 0, false);
    return iter;
	}

	void iter(std::function<void(const unsigned char*, uint32_t, V)> cb) { Node<V>::iter(root, cb); }

	long checkpoint() {
		// Returns size of checkpoint
		if (root->checkpointed) return 0;
		long result = 0;
		Node<V>::iter_nodes(root,
		                    [&result](Node<V>* n, int depth) -> bool {
			                    if (depth <= 1) {
				                    result += Node<V>::node_size(n);
				                    if (debug)
					                    std::cout << "Checkpointing node of type " << (int)n->type << " at depth "
					                              << depth << " into root file.\n";
				                    return true;
			                    } else {
				                    if (n->checkpointed) return false;
				                    if (debug)
					                    std::cout << "Checkpointing node of type " << (int)n->type << " at depth "
					                              << depth << " into subtree file.\n";
				                    Node<V>::iter_nodes(n,
				                                        [&result](Node<V>* n2, int depth2) -> bool {
					                                        (void)depth2;
					                                        result += Node<V>::node_size(n2);
					                                        return true;
				                                        },
				                                        0);
				                    n->checkpointed = true;
				                    return false;
			                    }
		                    },
		                    0);
		root->checkpointed = true;
		return result;
	};

	int max_depth() {
		int result = 0;
		Node<V>::iter_nodes(root,
		                    [&result](Node<V>* n, int depth) -> bool {
			                    (void)n;
			                    if (result < depth) result = depth;
			                    return true;
		                    },
		                    0);
		return result;
	};

	long node_size() {
		long result = 0;
		Node<V>::iter_nodes(root,
		                    [&result](Node<V>* n, int depth) -> bool {
			                    (void)depth;
			                    result += Node<V>::node_size(n);
			                    return true;
		                    },
		                    0);
		return result;
	};

	int destroy() {
		if (root)
			return Node<V>::decrement_refcount(root);
		else
			return 0;
	}

	double avg_stride() {
		long total_stride = 0;
		long num_nodes = 0;
		Node<V>* n_prev = NULL;
		Node<V>::iter_nodes(root,
		                    [&total_stride, &num_nodes, &n_prev](Node<V>* n, int depth) -> bool {
			                    (void)depth;
			                    if (n->type == LEAF) {
				                    if (n_prev != NULL) {
					                    long abs_stride = (char*)n - (char*)n_prev;
					                    //                        if (abs_stride < 0) abs_stride = -abs_stride;
					                    total_stride += abs_stride;
					                    if (num_nodes % 100000 == 0) {
						                    printf("%ld ", abs_stride);
					                    }
					                    if (abs_stride != 24 && num_nodes % 10 == 0) {
						                    printf("%ld ", abs_stride);
					                    }
				                    }
				                    n_prev = n;
				                    num_nodes++;
			                    }
			                    return true;
		                    },
		                    0);
		printf("\nnum nodes %ld\n", num_nodes);
		return (double)total_stride / num_nodes;
	}

	// private:
	Node<V>* root;
  Leaf<V>* tail = nullptr;
};

#include "fdbclient/part_impl.h"
