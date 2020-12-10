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

	struct result {
    KeyRef key;

    /*result& operator++() {
      ++it;
      return *this;
      }*/

    const KeyRef& operator*() const { return key; }

    const KeyRef& operator->() const { return key; }

    bool operator==(result const& k) const { return key == k.key; }
    bool operator != (result const& k) const { return key != k.key; }
  };

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
	result find(KeyRef const& k) {
    if (snapshots.back().search(k.begin(), k.size())) {
      return result{k};
    } else {
      return result{LiteralStringRef("")};
    }
  }
	result not_found() const { return result{LiteralStringRef("")}; }
  /*result begin() const { return result(s.atLatest().begin()); }
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
	partHarness tree;

	treeBenchmark(tree, [&arena]() { return randomStr(arena); });

	return Void();
}

void forceLinkVersionedMapTests() {}
