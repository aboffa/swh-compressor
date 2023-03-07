#include <list>
#include <string>
#include <fstream>
#include <iostream>

#include "minhash.h"
#include "SpookyV2.h"
#include "lsh_utils.h"

typedef unsigned __int128 uint128_t;

Minhash::hash_t Minhash::compute(const std::string &filename) {
    std::ifstream fin(filename, std::ifstream::in | std::ifstream::binary);
    if (!fin.good()) {
        std::cerr << "Error reading " << filename << std::endl;
        exit(EXIT_FAILURE);
    }
    Minhash::hash_t result;
    std::fill(begin(result), end(result), int64_t(-1));
    for (std::string line; std::getline(fin, line);) {
        // if line < 10 no
        // delete initial spaces and tabs
        // compute hash of the line
        std::string trimmed(trim(line));
        if (trimmed.size() >= 10) {
            for (auto i = 0; i < result.max_size(); i++) {
                result[i] = std::min<int64_t>(result[i], SpookyHash::Hash64(line.data(), line.size(), i));
            }
        }
    }
    return result;
}
