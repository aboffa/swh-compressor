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

template<size_t _NUM_PERMU = 32, typename T = int32_t>
class Minhash {
    /**
     * The type of all hashes.
     */
public:
    static const size_t NUM_PERMU = _NUM_PERMU;
    typedef T TYPE;

    typedef std::array<TYPE, NUM_PERMU> hash_t;

    static hash_t compute(const std::string &filename);
};
