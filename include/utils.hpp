#pragma once

#include <iostream>
#include <filesystem>
#include <vector>
#include <chrono>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "my_dataframe.h"
#include "rapidcsv.h"
#include "simhash.h"
#include "dkm_parallel.hpp"


using timer = std::chrono::high_resolution_clock;
std::error_code ec;

template<typename T>
T gray_code(T x) {
    return x ^ (x >> 1);
}

std::ostream &operator<<(std::ostream &o, uint128_t &to_print) {
    if (to_print >= (uint128_t(1) << 64)) {
        o << uint64_t(to_print >> 64);
    }
    o << uint64_t(to_print);
    return o;
}

const size_t NUM_THREAD = 32;
const std::string BLOBS_DIR = "/data/swh/blobs";

std::vector<std::pair<size_t, Simhash::hash_t>> get_simhashes_parallel(my_dataframe &df) {
    const size_t rowCount = df.get_num_files();
    std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec;
    lsh_vec.reserve(rowCount);
#pragma omp parallel num_threads(NUM_THREAD) shared(df)
    {
        std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec_private;
#pragma omp for nowait
        for (size_t i = 0; i < rowCount; ++i) {
            std::string sha1 = df.sha1_at_str(i);
            std::filesystem::path blob_hash(sha1);

            std::string filename_path(BLOBS_DIR / blob_hash);
            if (std::filesystem::is_regular_file(filename_path, ec)) {
                //it's a file
                Simhash::hash_t res = 0;
                res = Simhash::compute(filename_path);
                lsh_vec_private.emplace_back(i, res);
            } else {
                std::cerr << "ERROR -> " + ec.message() << std::endl;
                exit(EXIT_FAILURE);
            }
        }
#pragma omp critical
        lsh_vec.insert(lsh_vec.end(), lsh_vec_private.begin(), lsh_vec_private.end());
    }
    return lsh_vec;
}

void
compress_decompress_from_df(std::vector<size_t> &ordered_rows, std::string technique_name, std::string &dataset_name,
                            my_dataframe &df, std::string &compressor_path, size_t sorting_time,
                            std::string notes = "None") {
    std::string path_working_dir =
            "/data/swh/tmp._NEW_Sofware_Heritage_c++_" + technique_name + "_" + std::to_string(getpid());
    // no path and no flags
    std::string compressor_name = std::filesystem::path(compressor_path).filename();
    const size_t rowCount = df.get_num_files();
    size_t uncompressed_size = 0;
    std::filesystem::create_directory(path_working_dir);
    std::filesystem::current_path(path_working_dir);

    auto start = timer::now();
    std::string list_files_filename = path_working_dir + "/" + "list_files_compression.txt";
    std::ofstream file(list_files_filename);
    for (size_t i = 0; i < rowCount; ++i) {
        std::string sha1 = df.sha1_at_str(ordered_rows[i]);
        std::filesystem::path blob_hash(sha1);
        // write filenames in file
        std::string filename_path(BLOBS_DIR / blob_hash);
        uncompressed_size += std::filesystem::file_size(filename_path);
        file << filename_path.erase(0, 1) << std::endl;
    }
    double uncompressed_size_MiB = double(uncompressed_size) / double(1 << 20);

    std::cout << "File list: " << dataset_name << " of " << rowCount << " files of size (MiB) "
              << std::to_string(uncompressed_size_MiB) << std::endl;
    std::cout << "Technique: " << technique_name << "+" << compressor_name << std::endl << std::flush;

    // TODO: I should use std::filesystem::path instead of std::strings
    std::string generated_file = path_working_dir + "/" + technique_name + ".tar." + compressor_name;
    // run tar
    std::string to_run_compress =
            "tar -cf " + generated_file + " -C / -T " + list_files_filename + " -I" + compressor_path;
    int status = 0;
    status = system(to_run_compress.c_str());
    if (status != 0) {
        exit(EXIT_FAILURE);
    }

    size_t compressed_size = std::filesystem::file_size(generated_file);
    auto compressed_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();

    start = timer::now();
    std::string to_run_decompress = "tar -xf " + generated_file + " -I" + compressor_path;
    status = system(to_run_decompress.c_str());
    if (status != 0) {
        exit(EXIT_FAILURE);
    }

    auto decompressed_time = std::chrono::duration_cast<std::chrono::seconds>(timer::now() - start).count();

    status = system(std::string("rm -rf " + path_working_dir).c_str());
    if (status != 0) {
        exit(EXIT_FAILURE);
    }

    // print stats
    std::cout << "compression ratio (%): "
              << std::to_string(double(compressed_size) / double(uncompressed_size) * 100.)
              << std::endl;
    std::cout << "compression speed (MiB/s): "
              << std::to_string(double(uncompressed_size_MiB) / double(compressed_time + sorting_time))
              << std::endl;
    std::cout << "decompression speed (MiB/s): "
              << std::to_string(double(uncompressed_size_MiB) / double(decompressed_time)) << std::endl << std::flush;
}


std::vector<size_t> simhash_sort(my_dataframe &df) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec;
    lsh_vec.reserve(rowCount);
    for (size_t i = 0; i < rowCount; ++i) {
        std::string sha1 = df.sha1_at_str(i);
        std::filesystem::path blob_hash(sha1);

        std::string filename_path(BLOBS_DIR / blob_hash);
        if (std::filesystem::is_regular_file(filename_path, ec)) {
            Simhash::hash_t res = 0;
            res = Simhash::compute(filename_path);
            lsh_vec.emplace_back(i, res);
        } else {
            std::cerr << "ERROR -> " + ec.message() << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    std::sort(lsh_vec.begin(), lsh_vec.end(), [](auto &left, auto &right) {
        return left.second < right.second;
    });

    std::vector<size_t> to_return(rowCount);
    for (auto i = 0; i < rowCount; i++) {
        to_return[i] = lsh_vec[i].first;
    }
    return to_return;
}

std::vector<size_t> simhash_sort_p(my_dataframe &df) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec(get_simhashes_parallel(df));

    std::sort(lsh_vec.begin(), lsh_vec.end(), [](auto &left, auto &right) {
        return left.second < right.second;
    });
    std::vector<size_t> to_return(rowCount);
    for (auto i = 0; i < rowCount; i++) {
        to_return[i] = lsh_vec[i].first;
    }
    return to_return;
}

std::vector<size_t> simhash_sort_graycode(my_dataframe &df) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec(get_simhashes_parallel(df));

    std::sort(lsh_vec.begin(), lsh_vec.end(), [](auto &left, auto &right) {
        return gray_code(left.second) < gray_code(right.second);
    });

    std::vector<size_t> to_return(rowCount);
    for (auto i = 0; i < rowCount; i++) {
        to_return[i] = lsh_vec[i].first;
    }
    return to_return;
}

std::vector<size_t> simhash_cluster(my_dataframe &df, size_t div_for_cluster) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec(get_simhashes_parallel(df));
    std::vector<std::array<int32_t, 4>> lsh_vec_to_cluster(rowCount);
    //Simhash::hash_t ones_16 = (1 << 16) - 1;
    Simhash::hash_t ones_32 = (Simhash::hash_t(1) << 32) - 1;

    size_t full_size = 0;
    for (auto i = 0; i < rowCount; i++) {
        lsh_vec_to_cluster[i] = {int32_t(lsh_vec[i].second and ones_32),
                                 int32_t(lsh_vec[i].second >> 32 and ones_32),
                                 int32_t(lsh_vec[i].second >> 64 and ones_32),
                                 int32_t(lsh_vec[i].second >> 96 and ones_32)};
        full_size += df.length_at(i);
    }
    // should be function of the size of the elements
    const size_t num_cluster = size_t(double(full_size) / double(div_for_cluster)) + 2;
    auto cluster_data = dkm::kmeans_lloyd_parallel(lsh_vec_to_cluster, num_cluster);

    std::vector<std::vector<size_t>> clusters(num_cluster);
    size_t i = 0;
    for (const auto &label: std::get<1>(cluster_data)) {
        clusters[label].push_back(i++);
    }
    std::vector<size_t> merged_clusters;
    merged_clusters.reserve(rowCount);

    for (auto &items: clusters) {
        std::move(items.begin(), items.end(), std::back_inserter(merged_clusters));
    }
    return merged_clusters;
}

std::vector<size_t> filename_sort(my_dataframe &df) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::pair<size_t, std::string>> filename_vec;
    filename_vec.reserve(rowCount);

    for (auto i = 0; i < rowCount; i++) {
        filename_vec.emplace_back(i, df.filename_at(i));
    }

    // could add std::execution::par_unseq,
    std::sort(filename_vec.begin(), filename_vec.end(), [](auto &left, auto &right) {
        return left.second < right.second;
    });

    std::vector<size_t> to_return(rowCount);
    for (auto i = 0; i < rowCount; i++) {
        to_return[i] = filename_vec[i].first;
    }
    return to_return;
}