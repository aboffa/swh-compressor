#include <list>
#include <string>
#include <fstream>
#include <iostream>

#include "minhash.h"
#include "SpookyV2.h"
#include "lsh_utils.h"

typedef unsigned __int128 uint128_t;

template<> Minhash<>::hash_t Minhash<>::compute(const std::string &filename) {
    std::ifstream fin(filename, std::ifstream::in | std::ifstream::binary);
    if (!fin.good()) {
        std::cerr << "Error reading " << filename << std::endl;
        exit(EXIT_FAILURE);
    }
    Minhash::hash_t result;
    std::fill(begin(result), end(result), Minhash::TYPE(-1));
    for (std::string line; std::getline(fin, line);) {
        // if line < 10 no
        // delete initial spaces and tabs
        std::string trimmed(trim(line));
        if (trimmed.size() >= LINE_SIZE_THREASHOLD) {
            for (auto i = 0; i < Minhash::NUM_PERMU; i++) {
                // the library dkm::kmeans requires signed integers
                result[i] = std::min<Minhash::TYPE>(result[i], SpookyHash::Hash64(line.data(), line.size(), i));
            }
        }
    }
    return result;
}
