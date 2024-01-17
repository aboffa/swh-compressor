//
// Created by boffa on 10/02/23.
//
#include <iostream>
#include <vector>
#include <numeric>

#include "my_dataframe.h"
#include "utils.hpp"

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

    //std::vector<std::string> compressors = {"/usr/bin/zstd"};
    std::vector<std::string> compressors = {
            //"/home/boffa/bin/zstd_3_T16",
            "/home/boffa/bin/zstd_12_T16",
            //"/home/boffa/bin/zstd_19_T16",
            //"/home/boffa/bin/zstd_22_T16",
            //"/home/boffa/bin/zstd_22_T32",
            //"/home/boffa/bin/zstd_22_T64"
    };

    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << "Start computation at " << std::ctime(&now);
    std::cout << "Num threads (computing LSH and clustering): " << std::to_string(NUM_THREAD) << std::endl;
    for (int h = 1; h < argc; ++h) {
        std::string filename_path(argv[h]);
        std::size_t found = filename_path.find_last_of('/');
        // extract just the filename
        std::string filename = filename_path.substr(found + 1);

        // real test
        //size_t num_rows = 2346930;
        //size_t num_rows = 1146930;
        size_t num_rows = 646930;

        //debug
        //size_t num_rows = 12084;

        my_dataframe df(filename_path, num_rows);
        for (auto &compressor: compressors) {
            {
                const size_t rowCount = df.get_num_files();
                std::vector<size_t> ordered_rows(rowCount);
                std::iota(ordered_rows.begin(), ordered_rows.end(), 0); // already random
                compress_decompress_from_df(ordered_rows, "order_from_list", filename, df, compressor, 0);
            }
            {
                auto start = timer::now();
                std::vector<size_t> ordered_rows(filename_sort(df));
                assert(check_is_permutation(ordered_rows));
                auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
                // 2322 seconds are needed to get the filenames of this dataset
                compress_decompress_from_df(ordered_rows, "filename_sorted", filename, df, compressor,
                                            sorting_time);
            }
//            {
//                auto start = timer::now();
//                std::vector<size_t> ordered_rows(simhash_sort(df));
//                auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
//                compress_decompress_from_df(ordered_rows, "simhash_sort", filename, df, compressor, sorting_time);
//            }
            {
                auto start = timer::now();
                std::vector<size_t> ordered_rows(simhash_sort_p(df));
                auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
                compress_decompress_from_df(ordered_rows, "simhash_sort_parallel", filename, df, compressor,
                                            sorting_time);
            }
//            {
//                auto start = timer::now();
//                std::vector<size_t> ordered_rows(simhash_sort_graycode(df));
//                auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
//                compress_decompress_from_df(ordered_rows, "simhash_sort_graycode", filename, df, compressor,
//                                            sorting_time);
//            }
//            {
//                auto start = timer::now();
//                std::vector<size_t> ordered_rows(filename_simhash_hybrid_sort(df));
//                assert(check_is_permutation(ordered_rows));
//                auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
//                compress_decompress_from_df(ordered_rows, "filename_simhash_hybrid_sort", filename, df, compressor,
//                                            sorting_time);
//            }
//            {
//                auto start = timer::now();
//                std::vector<size_t> ordered_rows(simhash_cluster(df, 1 << 21));
//                auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
//                compress_decompress_from_df(ordered_rows, "simhash_cluster", filename, df, compressor, sorting_time);
//            }
//            for (auto mi: {500, 1000, 2000, 5000}) {
//                for (auto md: {10, 5, 2, 1}) {
//                    auto start = timer::now();
//                    std::vector<size_t> ordered_rows(minhash_cluster(df, 1 << 22, mi, md));
//                    auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
//                    compress_decompress_from_df(ordered_rows,
//                                                "minhash_cluster+" + std::to_string(mi) + "+" + std::to_string(md),
//                                                filename, df, compressor,
//                                                sorting_time);
//                }
//            }
//            {
//                auto start = timer::now();
//                std::vector<size_t> ordered_rows(mime_type_simhash_sort(df));
//                auto sorting_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();
//                compress_decompress_from_df(ordered_rows, "mime_type+simhash_sort", filename, df, compressor, sorting_time);
//            }
        }
    }
    auto end = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << "End computation at " << std::ctime(&end) << std::endl;
    return 0;
}