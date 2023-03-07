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

class Simhash {

    /**
     * The type of all hashes.
     */
    //uint64_t
public:
    typedef uint128_t hash_t;

    /**
     * The number of bits in a hash_t.
     */
    static const size_t BITS = sizeof(hash_t) * CHAR_BIT;

    static hash_t compute(const std::string &filename);
};
