#include "simhash.h"
#include "permutation.h"
#include "SpookyV2.h"

#include <algorithm>
#include <list>
#include <string>
#include <fstream>
#include <iostream>

typedef unsigned __int128 uint128_t;
const std::string WHITESPACE = " \n\r\t\f\v";

std::string ltrim(const std::string &s)
{
    size_t start = s.find_first_not_of(WHITESPACE);
    return (start == std::string::npos) ? "" : s.substr(start);
}

std::string rtrim(const std::string &s)
{
    size_t end = s.find_last_not_of(WHITESPACE);
    return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

std::string trim(const std::string &s) {
    return rtrim(ltrim(s));
}

size_t Simhash::num_differing_bits(Simhash::hash_t a, Simhash::hash_t b) {
    size_t count(0);
    Simhash::hash_t n = a ^ b;
    while (n) {
        ++count;
        n = n & (n - 1);
    }
    return count;
}

Simhash::hash_t Simhash::compute(const std::string &filename) {
    std::ifstream fin(filename, std::ifstream::in | std::ifstream::binary);
    if (!fin.good()) {
        std::cerr << "Error reading " << filename << std::endl;
        exit(EXIT_FAILURE);
    }
    // Initialize counts to 0
    std::vector<long> counts(Simhash::BITS, 0);
    std::unordered_set<Simhash::hash_t> hashes;
    for (std::string line; std::getline(fin, line);) {
        // if line < 10 no
        // delete initial spaces and tabs
        // compute hash of the line
        std::string trimmed(trim(line));
        if (trimmed.size() > 10) {
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
