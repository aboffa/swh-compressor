//
// Created by boffa on 10/02/23.
//
#include <iostream>
#include <vector>
#include <chrono>
#include <unistd.h>
#include <numeric>

#include "utils.hpp"
#include "rapidcsv.h"
#include "simhash.h"

void usage(char **argv) {
    std::cout << "Usage: " << argv[0] << " <filename1> <filename2> ..." << std::endl;
}


// "/home/boffa/swh-compressor/file_list/C_blobs.augmented.hash_sorted.csv"
// "/home/boffa/swh-compressor/file_list/C_blobs.augmented.hash_sorted_small.csv"
// "/home/boffa/swh-compressor/file_list/Python_blobs.augmented.hash_sorted.csv"

// "/data/swh/random_selection89GB.with_sha1_NEW.csv"

int main(int argc, char **argv) {
    if (argc <= 1) {
        usage(argv);
        exit(EXIT_FAILURE);
    }

    //std::vector<std::string> compressors = {"/home/boffa/bin/zstd3"};
    std::vector<std::string> compressors = {"/usr/bin/zstd"};

    for (int h = 1; h < argc; ++h) {
        std::string filename_path(argv[h]);
        std::size_t found = filename_path.find_last_of('/');
        // extract just the filename
        std::string filename = filename_path.substr(found + 1);
        rapidcsv::Document df(filename_path, rapidcsv::LabelParams(0, 0),
                              rapidcsv::SeparatorParams(',' /* pSeparator */,
                                                        false /* pTrim */,
                                                        rapidcsv::sPlatformHasCR /* pHasCR */,
                                                        false /* pQuotedLinebreaks */,
                                                        false /* pAutoQuote */));
        for (auto &compressor: compressors) {

            {
                const size_t rowCount = df.GetRowCount();
                std::vector<size_t> ordered_rows(rowCount);
                std::iota(ordered_rows.begin(), ordered_rows.end(), 0); // TODO here random
                compress_decompress_from_df(ordered_rows, "order_from_list", filename, df, compressor, 0);
            }
//            {
//                auto start = timer::now();
//                std::vector<size_t> ordered_rows(simhash_sort(df));
//                auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
//                compress_decompress_from_df(ordered_rows, "simhash_sort", filename, df, compressor, sorting_time);
//            }
//            {
//                auto start = timer::now();
//                std::vector<size_t> ordered_rows(simhash_sort_p(df));
//                auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
//                compress_decompress_from_df(ordered_rows, "simhash_sort_parallel", filename, df, compressor,
//                                            sorting_time);
//            }
//            {
//                auto start = timer::now();
//                std::vector<size_t> ordered_rows(simhash_sort_graycode(df));
//                auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
//                compress_decompress_from_df(ordered_rows, "simhash_sort_graycode", filename, df, compressor,
//                                            sorting_time);
//            }
//            {
//                auto start = timer::now();
//                std::vector<size_t> ordered_rows(simhash_cluster(df, 1 << 20));
//                auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
//                compress_decompress_from_df(ordered_rows, "simhash_cluster", filename, df, compressor, sorting_time);
//            }
        }
    }
    return 0;
}