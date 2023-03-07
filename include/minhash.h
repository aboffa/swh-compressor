#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <string>
#include <climits>

typedef unsigned __int128 uint128_t;

namespace Minhash {
    /**
     * The type of all hashes.
     */
    static const size_t NUM_PERMU = 64;
    typedef int64_t TYPE;

    typedef std::array<TYPE, NUM_PERMU> hash_t;

    hash_t compute(const std::string &filename);
}
