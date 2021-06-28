# CompileRocksDB

find_package(RocksDB)

include(ExternalProject)

if (RocksDB_FOUND)
  ExternalProject_Add(rocksdb
    SOURCE_DIR "${RocksDB_ROOT}"
    DOWNLOAD_COMMAND ""
    CMAKE_ARGS -DUSE_RTTI=1 -DPORTABLE=${PORTABLE_ROCKSDB}
               -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
               -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
               -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DWITH_GFLAGS=OFF
               -DWITH_TESTS=OFF
               -DWITH_TOOLS=OFF
               -DWITH_CORE_TOOLS=OFF
               -DWITH_BENCHMARK_TOOLS=OFF
               -DWITH_BZ2=OFF
               -DWITH_LZ4=ON
               -DWITH_SNAPPY=OFF
               -DWITH_ZLIB=OFF
               -DWITH_ZSTD=OFF
               -DROCKSDB_BUILD_SHARED=OFF
               -DCMAKE_POSITION_INDEPENDENT_CODE=True
    BUILD_BYPRODUCTS <BINARY_DIR>/librocksdb.a
    INSTALL_COMMAND ""
  )

  ExternalProject_Get_Property(rocksdb BINARY_DIR)
  set(ROCKSDB_LIBRARIES
      ${BINARY_DIR}/librocksdb.a)
else()
  ExternalProject_Add(rocksdb
    URL        https://github.com/facebook/rocksdb/archive/v6.20.3.tar.gz
    URL_HASH   SHA256=c6502c7aae641b7e20fafa6c2b92273d935d2b7b2707135ebd9a67b092169dca
    CMAKE_ARGS -DUSE_RTTI=1 -DPORTABLE=${PORTABLE_ROCKSDB}
               -DCMAKE_CXX_STANDARD=${CMAKE_CXX_STANDARD}
               -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
               -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
               -DWITH_GFLAGS=OFF
               -DWITH_TESTS=OFF
               -DWITH_TOOLS=OFF
               -DWITH_CORE_TOOLS=OFF
               -DWITH_BENCHMARK_TOOLS=OFF
               -DWITH_BZ2=OFF
               -DWITH_LZ4=ON
               -DWITH_SNAPPY=OFF
               -DWITH_ZLIB=OFF
               -DWITH_ZSTD=OFF
               -DROCKSDB_BUILD_SHARED=OFF
               -DCMAKE_POSITION_INDEPENDENT_CODE=True
    BUILD_BYPRODUCTS <BINARY_DIR>/librocksdb.a
    INSTALL_COMMAND ""
  )

  ExternalProject_Get_Property(rocksdb BINARY_DIR)
  set(ROCKSDB_LIBRARIES
      ${BINARY_DIR}/librocksdb.a)

  ExternalProject_Get_Property(rocksdb SOURCE_DIR)
  set (ROCKSDB_INCLUDE_DIR "${SOURCE_DIR}/include")

  set(ROCKSDB_FOUND TRUE)
endif()

message(STATUS "Found RocksDB library: ${ROCKSDB_LIBRARIES}")
message(STATUS "Found RocksDB includes: ${ROCKSDB_INCLUDE_DIR}")

mark_as_advanced(
    ROCKSDB_LIBRARIES
    ROCKSDB_INCLUDE_DIR
)
