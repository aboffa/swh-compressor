#include <list>
#include <string>
#include <fstream>
#include <iostream>

#include "simhash.h"
#include "SpookyV2.h"
#include "lsh_utils.h"

typedef unsigned __int128 uint128_t;

Simhash::hash_t Simhash::compute(const std::string &filename) {
    std::ifstream fin(filename, std::ifstream::in | std::ifstream::binary);
    if (!fin.good()) {
        std::cerr << "Error reading " << filename << std::endl;
        exit(EXIT_FAILURE);
    }
    // Initialize counts to 0
    std::vector<size_t> counts(Simhash::BITS, 0);
    std::unordered_set<Simhash::hash_t> hashes;
    for (std::string line; std::getline(fin, line);) {
        // if line < 10 no
        // delete initial spaces and tabs
        // compute hash of the line
        std::string trimmed(trim(line));
        if (trimmed.size() >= 10) {
            uint64_t hash1 = 0, hash2  = 0;
            Simhash::hash_t hash = 0;
            if constexpr (std::is_same_v<Simhash::hash_t, uint128_t>) {
                SpookyHash::Hash128(line.data(), line.size(), &hash1, &hash2);
                hash = ((uint128_t) hash1) << 64 | uint128_t(hash2);
            }
            else{
                hash = SpookyHash::Hash64(line.data(), line.size(), 42);
            }
            // Count the number of 1's, 0's in each position of the hashes
            for (size_t i = 0; i < BITS; ++i) {
                counts[i] += (hash & 1) ? 1 : -1;
                hash >>= 1;
            }
        }
    }

    // Produce the result
    Simhash::hash_t result(0);
    for (size_t i = 0; i < BITS; ++i) {
        if (counts[i] > 0) {
            result |= (static_cast<Simhash::hash_t>(1) << i);
        }
    }
    return result;
}
