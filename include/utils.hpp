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
#include "minhash.h"

using timer = std::chrono::high_resolution_clock;
std::error_code ec;

template<typename T>
T gray_code(T x) {
    return x ^ (x >> 1);
}

typedef unsigned __int128 uint128_t;

std::ostream &operator<<(std::ostream &o, uint128_t &to_print) {
    if (to_print >= (uint128_t(1) << 64)) {
        o << uint64_t(to_print >> 64);
    }
    o << uint64_t(to_print);
    return o;
}

const size_t NUM_THREAD = 16;
const std::string BLOBS_DIR = "/data/swh/blobs";
const std::string PROJECT_HOME = "/home/boffa/swh-compressor";

bool check_is_permutation(std::vector<size_t> v) {
    std::vector<size_t> incresing_integers(v.size());
    std::iota(incresing_integers.begin(), incresing_integers.end(), 0);
    return std::is_permutation(v.begin(), v.end(), incresing_integers.begin());
}

std::vector<std::pair<size_t, Simhash::hash_t>> get_simhashes_parallel(const my_dataframe &df) {
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
                if (std::filesystem::file_size(filename_path) < (1 << 20))
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

std::vector<std::pair<size_t, Simhash::hash_t>>
get_simhashes_parallel_list(const my_dataframe &df, std::vector<size_t> &indexes) {
    const size_t rowCount = df.get_num_files();
    std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec;
    lsh_vec.reserve(rowCount);
#pragma omp parallel num_threads(NUM_THREAD) shared(df)
    {
        std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec_private;
#pragma omp for nowait
        for (size_t i = 0; i < indexes.size(); ++i) {
            std::string sha1 = df.sha1_at_str(indexes[i]);
            std::filesystem::path blob_hash(sha1);

            std::string filename_path(BLOBS_DIR / blob_hash);
            if (std::filesystem::is_regular_file(filename_path, ec)) {
                //it's a file
                Simhash::hash_t res = 0;
                if (std::filesystem::file_size(filename_path) < (1 << 20))
                    res = Simhash::compute(filename_path);
                lsh_vec_private.emplace_back(indexes[i], res);
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

//std::vector<std::pair<size_t, Minhash<>::hash_t>> get_minhashes_parallel(const my_dataframe &df) {
//    const size_t rowCount = df.get_num_files();
//    std::vector<std::pair<size_t, Minhash<>::hash_t>> lsh_vec;
//    lsh_vec.reserve(rowCount);
//#pragma omp parallel num_threads(NUM_THREAD) shared(df)
//    {
//        std::vector<std::pair<size_t, Minhash::hash_t>> lsh_vec_private;
//#pragma omp for nowait
//        for (size_t i = 0; i < rowCount; ++i) {
//            std::string sha1 = df.sha1_at_str(i);
//            std::filesystem::path blob_hash(sha1);
//
//            std::string filename_path(BLOBS_DIR / blob_hash);
//            if (std::filesystem::is_regular_file(filename_path, ec)) {
//                //it's a file
//                Minhash::hash_t res = {0};
//                if (std::filesystem::file_size(filename_path) < (1 << 20))
//                    res = Minhash::compute(filename_path);
//                lsh_vec_private.emplace_back(i, res);
//            } else {
//                std::cerr << "ERROR -> " + ec.message() << std::endl;
//                exit(EXIT_FAILURE);
//            }
//        }
//#pragma omp critical
//        lsh_vec.insert(lsh_vec.end(), lsh_vec_private.begin(), lsh_vec_private.end());
//    }
//    return lsh_vec;
//}

void
compress_decompress_from_df(std::vector<size_t> &ordered_rows, std::string technique_name, std::string &dataset_name,
                            const my_dataframe &df, std::string &compressor_path, size_t sorting_time,
                            std::string notes = "None") {
    std::string path_working_dir =
            "/data/swh/tmp._NEW_Sofware_Heritage_c++_" + technique_name + "_" + std::to_string(getpid());
    // no path and no flags
    std::string compressor_name = std::filesystem::path(compressor_path).filename();
    const size_t rowCount = df.get_num_files();
    std::filesystem::create_directory(path_working_dir);
    std::filesystem::current_path(path_working_dir);

    auto start = timer::now();
    std::string list_files_filename = path_working_dir + "/" + "list_files_compression.txt";
    std::ofstream file(list_files_filename);

    size_t uncompressed_size = 0;
    for (size_t i = 0; i < rowCount; i++) {
        std::string sha1 = df.sha1_at_str(ordered_rows[i]);
        std::filesystem::path blob_hash(sha1);
        // write filenames in file
        std::string filename_path(BLOBS_DIR / blob_hash);
        uncompressed_size += std::filesystem::file_size(filename_path);
        file << filename_path.erase(0, 1) << std::endl;
    }
    assert(df.get_total_size() == uncompressed_size);

    double uncompressed_size_MiB = double(uncompressed_size) / double(1 << 20);

    std::cout << "File list: " << dataset_name << " from " << BLOBS_DIR << " of " << rowCount << " files of size (GiB) "
              << std::to_string(double(uncompressed_size) / double(1 << 30)) << std::endl;
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
              << std::to_string(double(uncompressed_size_MiB) / double(decompressed_time)) << std::endl << std::endl
              << std::flush;

    std::filesystem::current_path(PROJECT_HOME);
}


std::vector<size_t> simhash_sort(const my_dataframe &df) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec;
    lsh_vec.reserve(rowCount);
    for (size_t i = 0; i < rowCount; ++i) {
        std::string sha1 = df.sha1_at_str(i);
        std::filesystem::path blob_hash(sha1);

        std::string filename_path(BLOBS_DIR / blob_hash);
        if (std::filesystem::is_regular_file(filename_path, ec)) {
            Simhash::hash_t res = 0;
            if (std::filesystem::file_size(filename_path) < (1 << 20))
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
    for (size_t i = 0; i < rowCount; i++) {
        to_return[i] = lsh_vec[i].first;
    }
    return to_return;
}

std::vector<size_t> simhash_sort_p(const my_dataframe &df) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec(get_simhashes_parallel(df));

    std::sort(lsh_vec.begin(), lsh_vec.end(), [](auto &left, auto &right) {
        return left.second < right.second;
    });
    std::vector<size_t> to_return(rowCount);
    for (size_t i = 0; i < rowCount; i++) {
        to_return[i] = lsh_vec[i].first;
    }
    return to_return;
}

std::vector<size_t> simhash_sort_list(const my_dataframe &df, std::vector<size_t> &indexes) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec(get_simhashes_parallel_list(df, indexes));

    std::sort(lsh_vec.begin(), lsh_vec.end(), [](auto &left, auto &right) {
        return left.second < right.second;
    });
    std::vector<size_t> to_return(rowCount);
    for (size_t i = 0; i < rowCount; i++) {
        to_return[i] = lsh_vec[i].first;
    }
    return to_return;
}

std::vector<size_t> simhash_sort_graycode(const my_dataframe &df) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec(get_simhashes_parallel(df));

    std::sort(lsh_vec.begin(), lsh_vec.end(), [](auto &left, auto &right) {
        return gray_code(left.second) < gray_code(right.second);
    });

    std::vector<size_t> to_return(rowCount);
    for (size_t i = 0; i < rowCount; i++) {
        to_return[i] = lsh_vec[i].first;
    }
    return to_return;
}

std::vector<size_t> simhash_cluster(const my_dataframe &df, size_t div_for_cluster) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec(get_simhashes_parallel(df));
    std::vector<std::array<int32_t, 4>> lsh_vec_to_cluster(rowCount);
    //Simhash::hash_t ones_16 = (1 << 16) - 1;
    Simhash::hash_t ones_32 = (Simhash::hash_t(1) << 32) - 1;

    size_t full_size = 0;
    for (size_t i = 0; i < rowCount; i++) {
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

std::vector<size_t>
minhash_cluster(const my_dataframe &df, size_t div_for_cluster, size_t max_iteration = 2000, int min_delta = 5) {
    const size_t rowCount = df.get_num_files();

    //std::vector<std::pair<size_t, Minhash::hash_t>> lsh_vec(get_minhashes_parallel(df));
    std::vector<Minhash<>::hash_t> lsh_vec;

    for (size_t i = 0; i < rowCount; ++i) {
        std::string sha1 = df.sha1_at_str(i);
        std::filesystem::path blob_hash(sha1);

        std::string filename_path(BLOBS_DIR / blob_hash);
        if (std::filesystem::is_regular_file(filename_path, ec)) {
            //it's a file
            Minhash<>::hash_t res;
            std::fill(begin(res), end(res), Minhash<>::TYPE(-1));
            if (std::filesystem::file_size(filename_path) < (1 << 20))
                res = Minhash<>::compute(filename_path);
            lsh_vec.emplace_back(res);
        } else {
            std::cerr << "ERROR -> " + ec.message() << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    // should be function of the size of the elements
    const size_t num_cluster = size_t(double(df.get_total_size()) / double(div_for_cluster)) + 2;
    auto cp = dkm::clustering_parameters<Minhash<>::TYPE>(num_cluster);

    cp.set_max_iteration(max_iteration);
    cp.set_min_delta(min_delta);

    cp.set_random_seed(42);

    auto cluster_data = dkm::kmeans_lloyd_parallel(lsh_vec, cp);
    //auto cluster_data = dkm::kmeans_lloyd(lsh_vec, cp);

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

std::vector<size_t> filename_sort(const my_dataframe &df) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::tuple<size_t, size_t, std::string>> filename_vec;
    filename_vec.reserve(rowCount);

    for (size_t i = 0; i < rowCount; i++) {
        filename_vec.emplace_back(i, df.length_at(i), df.filename_at(i));
    }

    // could add std::execution::par_unseq,
    std::sort(filename_vec.begin(), filename_vec.end(), [](auto &left, auto &right) {
        if (get<2>(left) < get<2>(right)) return true;
        else if (get<2>(left) == get<2>(right))
            return get<1>(left) > get<1>(right);
        else
            return false;
    });

    std::vector<size_t> to_return(rowCount);
    for (size_t i = 0; i < rowCount; i++) {
        to_return[i] = get<0>(filename_vec[i]);
    }
    return to_return;
}

std::vector<size_t> filename_simhash_hybrid_sort(const my_dataframe &df) {
    const size_t rowCount = df.get_num_files();

    std::vector<std::tuple<size_t, size_t, std::string>> filename_vec;
    filename_vec.reserve(rowCount);

    for (size_t i = 0; i < rowCount; i++) {
        filename_vec.emplace_back(i, df.length_at(i), df.filename_at(i));
    }

    // could add std::execution::par_unseq,
    std::sort(filename_vec.begin(), filename_vec.end(), [](auto &left, auto &right) {
        if (get<2>(left) < get<2>(right)) return true;
        else if (get<2>(left) == get<2>(right))
            return get<1>(left) > get<1>(right);
        else
            return false;
    });

    // inside the blocks of files with same name, sort by LSH
    std::string curr_fn = get<2>(filename_vec[0]);
    std::vector<size_t> curr_indexes;
    std::vector<size_t> to_return;
    to_return.reserve(rowCount);
    curr_indexes.push_back(get<0>(filename_vec[0]));
    for (size_t i = 1; i < rowCount; i++) {
        curr_indexes.push_back(get<0>(filename_vec[i]));
        if (get<2>(filename_vec[i]) == curr_fn) {
            continue;
        } else {
            if (curr_indexes.size() > 5) {
                std::vector<std::pair<size_t, Simhash::hash_t>> lsh_vec(get_simhashes_parallel_list(df, curr_indexes));
                std::sort(lsh_vec.begin(), lsh_vec.end(), [](auto &left, auto &right) {
                    return left.second < right.second;
                });
                for (size_t j = 0; j < curr_indexes.size(); j++) {
                    curr_indexes[j] = lsh_vec[j].first;
                }
            }
            for (size_t j = 0; j < curr_indexes.size(); j++) {
                to_return.push_back(curr_indexes[j]);
            }
            curr_indexes.clear();
        }
    }
    return to_return;
}

std::vector<size_t> mime_type_simhash_sort(const my_dataframe &df) {
    const size_t rowCount = df.get_num_files();

    std::vector<size_t> to_return(rowCount);
}
