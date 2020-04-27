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

	void insert(K const& k) { s.insert(k, 1); }
	result find(K const& k) const { return result(s.atLatest().find(k)); }
	result not_found() const { return result(s.atLatest().end()); }
	result begin() const { return result(s.atLatest().begin()); }
	result end() const { return result(s.atLatest().end()); }
	result lower_bound(K const& k) const { return result(s.atLatest().lower_bound(k)); }
	result upper_bound(K const& k) const { return result(s.atLatest().upper_bound(k)); }
	void erase(K const& k) { s.erase(k); }
};

TEST_CASE("performance/map/int/VersionedMap") {
    VersionedMapHarness<int> tree;

	treeBenchmark(tree, *randomInt);

	return Void();
}

TEST_CASE("performance/map/StringRef/VersionedMap") {
	Arena arena;
    VersionedMapHarness<StringRef> tree;
    
	treeBenchmark(tree, [&arena]() { return randomStr(arena); });

	return Void();
}

#include "fdbserver/art.h"
#include "fdbserver/art_impl.h"

struct artHarness {
	using map = art_tree;
	using key_type = KeyRef;

	struct result {
		art_iterator it;

		result(art_iterator it) : it(it) {}

		result& operator++() {
			++it;
			return *this;
		}

		const KeyRef& operator*() const { return it.key(); }

		const KeyRef& operator->() const { return it.key(); }

		bool operator==(result const& k) const { return it == k.it; }
		bool operator!=(result const& k) const { return it != k.it; }
	};

	map s;

	art_iterator find_impl(const KeyRef& k) const {
		auto it = s.lower_bound(k);
		if (it != art_iterator() && it.key() == k) {
			return it;
		}
		return art_iterator();
	}
	void insert(KeyRef const& k) { s.insert(k, nullptr); }
	result find(KeyRef const& k) const { return result(find_impl(k)); }
	result not_found() const { return result(art_iterator()); }
	// result begin() const { return result(s.atLatest().begin()); }
	result end() const { return result(art_iterator()); }
	result lower_bound(KeyRef const& k) const { return result(s.lower_bound(k)); }
	result upper_bound(KeyRef const& k) const { return result(s.upper_bound(k)); }
	void erase(KeyRef const& k) {
		auto it = find_impl(k);
		if (it != art_iterator()) {
			s.erase(it);
		}
	}
};

TEST_CASE("performance/map/StringRef/art") {
	Arena arena;
	artHarness tree{ art_tree(arena) };

	treeBenchmark(tree, [&arena]() { return randomStr(arena); });

	return Void();
}

#include "fdbclient/part.h"

struct partHarness {
	using map = ArtTree<int>;
	using key_type = KeyRef;

	/*struct result {
	    part_iterator it;

	      result(part_iterator it) : it(it) {}

	      result& operator++() {
	          ++it;
	          return *this;
	      }

	      const KeyRef& operator*() const { return it.key(); }

	      const KeyRef& operator->() const { return it.key(); }

	      bool operator==(result const& k) const { return it == k.it; }
	    bool operator != (result const& k) const { return it != k.it; }
	    };*/

	map s;

	void insert(KeyRef const& k) { s.insert(k.begin(), k.size(), 1337); }
	/*result find(KeyRef const& k) const { return result(find_impl(k)); }
	result not_found() const { return result(part_iterator()); }
	result begin() const { return result(s.atLatest().begin()); }
	result end() const { return result(part_iterator()); }
	result lower_bound(KeyRef const& k) const { return result(s.lower_bound(k)); }
	result upper_bound(KeyRef const& k) const { return result(s.upper_bound(k)); }
	void erase(KeyRef const& k) {
	  auto it = find_impl(k);
	  if (it != part_iterator()) {
	    s.erase(it);
	  }
	  }*/
};

TEST_CASE("performance/map/StringRef/part") {
	Arena arena;
	partHarness tree{ ArtTree<int>() };

	treeBenchmark(tree, [&arena]() { return randomStr(arena); });

	return Void();
}

void forceLinkVersionedMapTests() {}
