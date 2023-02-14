#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <string>

typedef unsigned __int128 uint128_t;

namespace Simhash {

    /**
     * The type of all hashes.
     */
    //uint64_t
    typedef uint128_t hash_t;

    /**
     * The type of a match of two hashes.
     */
    typedef std::pair<hash_t, hash_t> match_t;

    /**
     * For use with matches_t.
     */
    struct match_t_hash {
        inline std::size_t operator()(const std::pair<hash_t, hash_t> &v) const {
            return static_cast<hash_t>(v.first * 31 + v.second);
        }
    };

    /**
     * The type for matches what we've returned.
     */
    typedef std::unordered_set<match_t, match_t_hash> matches_t;

    /**
     * The type of a set of clusters.
     */
    typedef std::unordered_set<hash_t> cluster_t;
    typedef std::vector<cluster_t> clusters_t;

    /**
     * The number of bits in a hash_t.
     */
    static const size_t BITS = sizeof(hash_t) * 8;

    /**
     * Compute the number of bits that are flipped between two numbers
     *
     * @param a - reference number
     * @param b - number to compare
     *
     * @return number of bits that differ between a and b */
    size_t num_differing_bits(hash_t a, hash_t b);

    hash_t compute(const std::string &filename);
}
