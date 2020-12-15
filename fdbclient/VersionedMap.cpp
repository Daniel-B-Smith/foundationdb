#include "fdbclient/VersionedMap.h"
#include "flow/TreeBenchmark.h"
#include "flow/UnitTest.h"

template <typename K>
struct VersionedMapHarness {
	using map = VersionedMap<K, int>;
	using key_type = K;

	struct result {
		typename map::iterator it;

		result(typename map::iterator it) : it(it) {}

		result& operator++() {
			++it;
			return *this;
		}

		const K& operator*() const { return it.key(); }

		const K& operator->() const { return it.key(); }

		bool operator==(result const& k) const { return it == k.it; }
	};

	map s;

	void insert(K const& k, int version) { s.insert(k, version, 1); }
	result find(K const& k) const { return result(s.atLatest().find(k)); }
	result not_found() const { return result(s.atLatest().end()); }
	result begin() const { return result(s.atLatest().begin()); }
	result end() const { return result(s.atLatest().end()); }
	result lower_bound(K const& k) const { return result(s.atLatest().lower_bound(k)); }
	result upper_bound(K const& k) const { return result(s.atLatest().upper_bound(k)); }
	void erase(K const& k) { s.erase(k); }
};

/*TEST_CASE("performance/map/int/VersionedMap") {
    VersionedMapHarness<int> tree;

	treeBenchmark(tree, *randomInt);

	return Void();
  }*/

TEST_CASE("performance/map/StringRef/VersionedMap") {
	Arena arena;
  VersionedMapHarness<StringRef> tree;

	treeBenchmark(tree, [&arena]() { return randomStr(arena); });

	return Void();
}

#include "fdbclient/part.h"

struct partHarness {
	using map = ArtTree<int>;
	using key_type = KeyRef;
  using iterator = ArtTree<int>::iterator;

  partHarness() {
    snapshots.reserve(1000000);
    snapshots.push_back(map());
  }

  std::vector<map> snapshots;

	void insert(KeyRef const& k, int version) {
    version += 1;
    ASSERT_GE(version, snapshots.size());
    if (version > snapshots.size()) {
      snapshots.push_back(snapshots.back().snapshot());
    }
    snapshots.back().insert(k.begin(), k.size(), 1337);
  }
	iterator find(KeyRef const& k) {
    iterator iter;
    iter.leaf = snapshots.back().search(k.begin(), k.size());
    return iter;
  }
	iterator not_found() const { return {}; }
  /*result begin() const { return result{Node<int>::minimum(snapshots.back().root)}; }
    result end() const { return not_found(); }*/
	iterator lower_bound(KeyRef const& k) {
    return snapshots.back().lower_bound(k.begin(), k.size());
  }
	iterator upper_bound(KeyRef const& k) {
    return snapshots.back().upper_bound(k.begin(), k.size());
  }

    /*void erase(KeyRef const& k) {
	  auto it = find_impl(k);
	  if (it != part_iterator()) {
	    s.erase(it);
	  }
	  }*/
};

TEST_CASE("performance/map/StringRef/part") {
	Arena arena;
	partHarness tree;

	treeBenchmark(tree, [&arena]() { return randomStr(arena); });

	return Void();
}

TEST_CASE("performance/map/unit_test/lower_bound") {
  Arena arena;
  ArtTree<int> tree;

	int keyCount = 1000000;

	std::vector<KeyRef> keys;
  std::vector<KeyRef> sorted;

  for (int i = 0; i < keyCount; i++) {
		keys.push_back(randomStr(arena));
    sorted.push_back(keys.back());
  }

	std::sort(sorted.begin(), sorted.end());
	sorted.resize(std::unique(sorted.begin(), sorted.end()) - sorted.begin());

  for (int i = 0; i < keyCount; i++) {
    tree.insert(keys[i].begin(), keys[i].size(), 1337);
  }

  for (int i = 0; i < sorted.size(); i++) {
    auto it = tree.lower_bound(sorted[i].begin(), sorted[i].size());
    ASSERT(it);
    ASSERT(*it == sorted[i]);
  }

  int randomReads = 10000;

  for (int i = 0; i < randomReads; i++) {
    auto key = randomStr(arena);
    auto it = std::lower_bound(sorted.begin(), sorted.end(), key);
    if (it == sorted.end()) {
      ASSERT(!tree.lower_bound(key.begin(), key.size()));
    } else {
      auto lb = tree.lower_bound(key.begin(), key.size());
      ASSERT(lb);
      if (*lb != *it) {
        std::cout << "key: " << key.printable() << "\n";
        std::cout << "expecting: " << it->printable() << "\n";
        debug = true;
        tree.lower_bound(key.begin(), key.size());
        ASSERT(false);
      }
    }
  }

  // upper_bound
  for (int i = 0; i < randomReads; i++) {
    auto key = randomStr(arena);
    auto it = std::upper_bound(sorted.begin(), sorted.end(), key);
    if (it == sorted.end()) {
      ASSERT(!tree.upper_bound(key.begin(), key.size()));
    } else {
      auto ub = tree.upper_bound(key.begin(), key.size());
      ASSERT(ub);
      ASSERT(*ub == *it);
    }
  }

  // iteration
  // TODO: Add proper `begin()`
  /*auto* l = Node<int>::minimum(tree.root);
  auto s = sorted.begin();
  while (l && s != sorted.end()) {
    ASSERT(KeyRef(l->key, l->key_len) == *s);
    ++s;
    l = l->next;
  }
  ASSERT(!l);
  ASSERT(s == sorted.end());*/

  return Void();
}

void forceLinkVersionedMapTests() {}
