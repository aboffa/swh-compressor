//
// Created by boffa on 16/01/24.
//

// THIS MUST BE PLACES INTO ROCKSD EXAMPLE DIR

#include <cmath>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

#include "../include/swh-compressor/include/my_dataframe.h"
#include "../include/swh-compressor/include/utils.hpp"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_manager.h"

const std::string INPUT_DIR = "/data/swh/blobs_uncompressed";
const std::string OUTPUT_DIR = "/extralocal/swh";

std::string exec(const char *cmd) {
    std::array<char, 128> buffer;
    std::string result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
    if (!pipe) {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    return result;
}

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

void usage(char **argv) {
    std::cout << "Usage: " << argv[0] << " <filename1> <filename2> ..."
              << std::endl;
}

double round_to(double value, double precision = 1.0) {
    return std::round(value / precision) * precision;
}

std::string from_compression_enum_to_string(
        rocksdb::CompressionType compression) {
    switch (compression) {
        case 0:
            return "kNoCompression";
        case 1:
            return "kSnappyCompression";
        case 2:
            return "kZlibCompression";
        case 3:
            return "kBZip2Compression";
        case 4:
            return "kLZ4Compression";
        case 5:
            return "kLZ4HCCompression";
        case 6:
            return "kXpressCompression";
        case 7:
            return "kZSTD";
        default:
            return "Unknown";
    }
}

rocksdb::CompressionType from_compressor_string_to_enum(
        std::string &compressor) {
    if (compressor == "kNoCompression") {
        return rocksdb::kNoCompression;
    } else if (compressor == "kSnappyCompression") {
        return rocksdb::kSnappyCompression;
    } else if (compressor == "kZlibCompression") {
        return rocksdb::kZlibCompression;
    } else if (compressor == "kBZip2Compression") {
        return rocksdb::kBZip2Compression;
    } else if (compressor == "kLZ4Compression") {
        return rocksdb::kLZ4Compression;
    } else if (compressor == "kLZ4HCCompression") {
        return rocksdb::kLZ4HCCompression;
    } else if (compressor == "kXpressCompression") {
        return rocksdb::kXpressCompression;
    } else if (compressor == "kZSTD") {
        return rocksdb::kZSTD;
    } else {
        std::cout << "Unknown compressor " << compressor << std::endl;
        exit(EXIT_FAILURE);
    }
}

const double KiB = 1024.0;
const double MiB = 1024.0 * 1024.0;
const double GiB = 1024.0 * 1024.0 * 1024.0;

const size_t n_zero = 10;
const size_t div_num_queries = 10;

double test_random_access(rocksdb::DB *db, std::vector<size_t> &queries) {
    auto start = timer::now();
    size_t decompressed_bytes = 0;
    for (size_t i = 0; i < queries.size(); i++) {
        std::string random_key = std::to_string(i);
        std::string key =
                std::string(n_zero - std::min(n_zero, random_key.length()), '0') +
                random_key;
        std::string value;
        rocksdb::Status s = db->Get(rocksdb::ReadOptions(), key, &value);
        decompressed_bytes += value.size();
        if (not s.ok()) {
            std::cout << "Error getting key " << key << std::endl;
            std::cout << s.ToString() << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    auto stop = timer::now();
    auto full_random_access_time =
            std::chrono::duration_cast<std::chrono::seconds>(stop - start).count();
    return double(decompressed_bytes / MiB) / double(full_random_access_time);
}

void load_in_rocksDB(const my_dataframe &df, std::vector<size_t> &ordered_rows,
                     std::string sorting_name, std::string dataset_name,
                     std::string &compressor, int compression_level,
                     size_t block_size) {  //,
    // const std::function<std::string(const my_dataframe &, size_t)> &f) {
    const size_t rowCount = df.get_num_files();
    rocksdb::DB *db;

    rocksdb::Options options;

    // options.write_buffer_size = block_size;

    // options.max_bytes_for_level_base = 2 * block_size;

    // Create a BlockBasedTableOptions object
    rocksdb::BlockBasedTableOptions table_options;

    // Set the desired block size (in bytes)
    table_options.block_size =
            block_size;  // For example, setting the block size to 1MiB

    // Now I dont care exactly how big the block,sst,levels are.
    // I just need the compressors to actually work on MiB of data.

    // Set the table factory for the options
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    auto sst_file_manager = rocksdb::NewSstFileManager(rocksdb::Env::Default());

    // Associate the SstFileManager with the options
    options.sst_file_manager =
            std::shared_ptr<rocksdb::SstFileManager>(sst_file_manager);

    // https://github.com/facebook/rocksdb/wiki/Compression
    // options.compression = rocksdb::kNoCompression;
    options.compression = from_compressor_string_to_enum(compressor);
    options.bottommost_compression = from_compressor_string_to_enum(compressor);

    rocksdb::CompressionOptions zstd_compression_options;
    zstd_compression_options.level = compression_level;

    options.compression_opts = zstd_compression_options;

    const std::string kDBPath =
            std::filesystem::path(OUTPUT_DIR) /
            std::filesystem::path("tmp_rocksdb_" + std::to_string(getpid()));

    auto start = timer::now();

    options.create_if_missing = true;
    rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &db);
    if (not s.ok()) {
        std::cout << "Error opening DB" << std::endl;
        std::cout << s.ToString() << std::endl;
        exit(EXIT_FAILURE);
    }
    size_t size_from_filesystem = 0;
    size_t size_from_dataframe = df.get_total_size();
    // std::cout << "DB opened" << std::endl;
    for (size_t i = 0; i < rowCount; i++) {
        size_t pemuted_i = ordered_rows[i];
        std::string filename = df.sha1_at_str(pemuted_i);
        std::string local_path = df.local_path_at_str(pemuted_i);
        std::string file_path = std::filesystem::path(INPUT_DIR) /
                                std::filesystem::path(local_path) /
                                std::filesystem::path(filename);
        // std::string key = df.sha1_at_str(pemuted_i);
        std::string key_tmp = std::to_string(i);
        std::string key =
                std::string(n_zero - std::min(n_zero, key_tmp.length()), '0') + key_tmp;

        // TODO: filter by size (avoid very big files?)
        // read file content
        std::ifstream t(file_path);
        if (t.is_open()) {
            std::stringstream buffer;
            buffer << t.rdbuf();
            std::string value = buffer.str();
            size_from_filesystem += value.size();
            s = db->Put(rocksdb::WriteOptions(), key, value);
            //      if (i < 10) {
            //        std::cout << i << ") key: " << key << std::endl;
            //        std::cout << "______________________________" << std::endl;
            //        std::cout << "value[0:100]: " << value.substr(0, 100) <<
            //        std::endl; std::cout << "______________________________" <<
            //        std::endl; std::cout << "filepath: " << file_path << std::endl;
            //        std::cout << "filename: " << df.filename_at(pemuted_i) <<
            //        std::endl;
            //      }
            if (not s.ok()) {
                std::cout << "Error inserting key " << key << std::endl;
                std::cout << "value[0:100]: " << value.substr(0, 100) << std::endl;
                std::cout << s.ToString() << std::endl;
                exit(EXIT_FAILURE);
            }
        } else {
            std::cout << "Error opening file " << file_path << std::endl;
            exit(EXIT_FAILURE);
        }
    }

    db->CompactFiles(rocksdb::CompactionOptions(), {""}, 1);
    db->Flush(rocksdb::FlushOptions());

    auto stop = timer::now();
    auto loading_time =
            std::chrono::duration_cast<std::chrono::seconds>(stop - start).count();

    const double round_precision = 0.0001;

    assert(size_from_filesystem == size_from_dataframe);
    std::cout
            << "- load_in_rocksDB Sorting technique (with compacting and fflush()): "
            << sorting_name << ". Dataset " << dataset_name << ". Compression "
            << from_compression_enum_to_string(options.compression) << " with level "
            << compression_level << ". table_options.block_size = "
            << double(table_options.block_size) / 1024 << " KiB." << std::endl;

    std::cout << "-- Number of files: " << rowCount << std::endl;

    uint64_t total_sst_size = 0;
    // Get a list of all SST files in the database directory
    rocksdb::Env *env = rocksdb::Env::Default();
    std::vector<std::string> sst_files;
    s = env->GetChildren(kDBPath, &sst_files);
    if (!s.ok()) {
        std::cerr << "Error getting SST files: " << s.ToString() << std::endl;
        exit(1);
    }

    // Calculate the total size of all SST files
    for (const std::string &sst_file : sst_files) {
        if (sst_file.rfind(".sst") != std::string::npos) {
            uint64_t fileSize;
            s = env->GetFileSize(kDBPath + "/" + sst_file, &fileSize);
            if (!s.ok()) {
                std::cerr << "Error getting file size for " << sst_file << ": "
                          << s.ToString() << std::endl;
                exit(1);
            }
            total_sst_size += fileSize;
        }
    }

    // uint64_t total_sst_size = sst_file_manager->GetTotalSize();
    const double size_GiB = double(size_from_dataframe) / GiB;
    const double total_sst_size_GiB = double(total_sst_size) / GiB;
    std::cout << "-- Dataset size: " << size_from_dataframe << " bytes. "
              << round_to(size_GiB, round_precision) << " GiB." << std::endl;
    std::cout << "-- Total SST size: " << total_sst_size << " bytes. "
              << round_to(total_sst_size_GiB, round_precision) << " GiB."
              << std::endl;
    std::cout << "-- Num sst_files " << sst_files.size() << " of avg size (KiB) "
              << total_sst_size / sst_files.size() / KiB << std::endl;
    // std::cout << "-- Num levels " << sst_file_manager->GetNumLevels() <<
    // std::endl;

    //  int max_levels =
    //      options.num_levels;  // Assuming 7 levels, but this can vary. You can
    //                           // adjust it based on your configuration.
    //  for (int level = 0; level < max_levels; ++level) {
    //    std::string property = "rocksdb.num-files-at-level" +
    //    std::to_string(level); std::string value; if (db->GetProperty(property,
    //    &value)) {
    //      std::cout << "-- Level " << level << ": " << value << " files"
    //                << std::endl;
    //    } else {
    //      std::cerr << "Failed to get property for level " << level <<
    //      std::endl;
    //    }
    //  }

    std::cout << "-- Compression ratio (compressed/uncompressed): "
              << round_to((total_sst_size_GiB / size_GiB) * 100, round_precision)
              << "%" << std::endl;

    size_t space_remapping_byte = rowCount * (32 + 32 + 5) / 8;
    std::cout << "-- Upper bound space remapping: " << space_remapping_byte
              << " bytes. "
              << round_to(double(space_remapping_byte) / GiB, round_precision)
              << " GiB." << std::endl;
    std::cout << "-- Loading time: " << loading_time << " seconds" << std::endl
              << std::flush;
    std::cout << "-- Loading speed " << (total_sst_size / MiB) / loading_time
              << " MiB/s" << std::endl
              << std::flush;

    std::vector<size_t> queries(rowCount);
    std::iota(queries.begin(), queries.end(), 0);
    std::mt19937 g2(21);
    std::shuffle(queries.begin(), queries.end(), g2);
    queries.resize(rowCount / div_num_queries);
    double full_random_access_speed = test_random_access(db, queries);

    std::cout << "-- Random access speed: " << full_random_access_speed
              << " MiB/s" << std::endl
              << std::flush;

    delete db;

    std::string to_run_remove = "rm -rf " + kDBPath;
    int status = system(to_run_remove.c_str());
    if (status != 0) {
        std::cout << "Failed to run " << to_run_remove << std::endl;
        exit(EXIT_FAILURE);
    }
    // std::cout << "DB closed" << std::endl;
}

// witout memtables and without compaction, create SST tables by my own
void bulk_load_in_rocksDB(const my_dataframe &df,
                          std::vector<size_t> &ordered_rows,
                          std::string sorting_name, std::string dataset_name,
                          size_t sst_size, std::string &compressor,
                          int compression_level) {
    const size_t rowCount = df.get_num_files();
    rocksdb::DB *db;

    rocksdb::Options options;

    auto sst_file_manager = rocksdb::NewSstFileManager(rocksdb::Env::Default());

    // Associate the SstFileManager with the options
    options.sst_file_manager =
            std::shared_ptr<rocksdb::SstFileManager>(sst_file_manager);

    // https://github.com/facebook/rocksdb/wiki/Compression
    // options.compression = rocksdb::kNoCompression;
    options.compression = from_compressor_string_to_enum(compressor);
    options.bottommost_compression = from_compressor_string_to_enum(compressor);

    rocksdb::CompressionOptions zstd_compression_options;
    zstd_compression_options.level = compression_level;

    options.compression_opts = zstd_compression_options;

    const std::string kDBPath =
            std::filesystem::path(OUTPUT_DIR) /
            std::filesystem::path("tmp_rocksdb_" + std::to_string(getpid()));

    auto start = timer::now();

    options.create_if_missing = true;
    rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &db);

    if (not s.ok()) {
        std::cout << "Error opening DB" << std::endl;
        std::cout << s.ToString() << std::endl;
        exit(EXIT_FAILURE);
    }

    size_t size_from_dataframe = df.get_total_size();
    size_t size_from_filesystem = 0;

    size_t sst_index = 0;
    size_t this_sst_size = 0;
    size_t this_file_size = 0;

    std::filesystem::path my_sst_dir =
            std::filesystem::path(OUTPUT_DIR) /
            std::filesystem::path("tmp_rocksdb_" + std::to_string(getpid())) /
            std::filesystem::path("ssts");
    // create directory
    std::filesystem::create_directories(my_sst_dir);

    // Create a BlockBasedTableOptions object
    rocksdb::BlockBasedTableOptions table_options;

    // Just one block per SST table
    table_options.block_size =
            sst_size;  // For example, setting the block size to 1MiB

    // Now I dont care exactly how big the block,sst,levels are.
    // I just need the compressors to actually work on MiB of data.

    // Set the table factory for the options
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));


    std::string new_sst_file_path =
            std::filesystem::path(OUTPUT_DIR) /
            std::filesystem::path("tmp_rocksdb_" + std::to_string(getpid())) /
            std::filesystem::path("ssts") /
            std::filesystem::path("sst_" + std::to_string(sst_index) + ".sst");

    rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions(), options,
                                           options.comparator);


    s = sst_file_writer.Open(new_sst_file_path);
    assert(s.ok());

    // Insert rows into the SST file, note that inserted keys must be
    // strictly increasing (based on options.comparator)
    for (size_t i = 0; i < rowCount; i++) {
        size_t pemuted_i = ordered_rows[i];
        std::string filename = df.sha1_at_str(pemuted_i);
        std::string local_path = df.local_path_at_str(pemuted_i);
        std::string file_path = std::filesystem::path(INPUT_DIR) /
                                std::filesystem::path(local_path) /
                                std::filesystem::path(filename);
        // std::string key = df.sha1_at_str(pemuted_i);
        std::string key_tmp = std::to_string(i);
        std::string key =
                std::string(n_zero - std::min(n_zero, key_tmp.length()), '0') + key_tmp;

        // read file content
        std::ifstream t(file_path);
        if (t.is_open()) {
            std::stringstream buffer;
            buffer << t.rdbuf();
            std::string value = buffer.str();
            this_file_size = value.size();
            this_sst_size += this_file_size;
            size_from_filesystem += this_file_size;
            s = sst_file_writer.Put(key, value);
            assert(s.ok());

            if (this_sst_size > sst_size or i == rowCount - 1) {
                sst_file_writer.Finish();
                // Ingest the external SST file into the DB
                s = db->IngestExternalFile({new_sst_file_path},
                                           rocksdb::IngestExternalFileOptions());
                assert(s.ok());

                if (i != rowCount - 1) {
                    sst_index++;
                    this_sst_size = 0;
                    new_sst_file_path = std::filesystem::path(
                            my_sst_dir / std::filesystem::path(
                                    "sst_" + std::to_string(sst_index) + ".sst"));
                    s = sst_file_writer.Open(new_sst_file_path);
                    assert(s.ok());
                }
            }
        } else {
            std::cout << "Error opening file " << file_path << std::endl;
            exit(EXIT_FAILURE);
        }
        t.close();
    }

    auto stop = timer::now();
    auto loading_time =
            std::chrono::duration_cast<std::chrono::seconds>(stop - start).count();

    const double round_precision = 0.0001;

    assert(size_from_filesystem == size_from_dataframe);
    std::cout << "- bulk_load_in_rocksDB Sorting technique: " << sorting_name
              << ". Dataset " << dataset_name << ". Compression "
              << from_compression_enum_to_string(options.compression)
              << " with level " << compression_level << ". SST size "
              << double(sst_size) / 1024 << " KiB." << std::endl;
    std::cout << "-- Number of files: " << rowCount << std::endl;

    uint64_t total_sst_size = 0;
    // Get a list of all SST files in the database directory
    rocksdb::Env *env = rocksdb::Env::Default();
    std::vector<std::string> sst_files;
    s = env->GetChildren(kDBPath, &sst_files);
    if (!s.ok()) {
        std::cerr << "Error getting SST files: " << s.ToString() << std::endl;
        exit(1);
    }

    // Calculate the total size of all SST files
    for (const std::string &sst_file : sst_files) {
        if (sst_file.rfind(".sst") != std::string::npos) {
            uint64_t fileSize;
            s = env->GetFileSize(kDBPath + "/" + sst_file, &fileSize);
            if (!s.ok()) {
                std::cerr << "Error getting file size for " << sst_file << ": "
                          << s.ToString() << std::endl;
                exit(1);
            }
            total_sst_size += fileSize;
        }
    }

    // uint64_t total_sst_size = sst_file_manager->GetTotalSize();
    const double size_GiB = double(size_from_dataframe) / GiB;
    const double total_sst_size_GiB = double(total_sst_size) / GiB;
    std::cout << "-- Dataset size: " << size_from_dataframe << " bytes. "
              << round_to(size_GiB, round_precision) << " GiB." << std::endl;
    std::cout << "-- Total SST size: " << total_sst_size << " bytes. "
              << round_to(total_sst_size_GiB, round_precision) << " GiB."
              << std::endl;
    std::cout << "-- Num sst_files " << sst_files.size() << " of avg size (KiB) "
              << total_sst_size / sst_files.size() / KiB << std::endl;
    // std::cout << "-- Num levels " << sst_file_manager->GetNumLevels() <<
    // std::endl;

    std::cout << "-- Compression ratio (compressed/uncompressed): "
              << round_to((total_sst_size_GiB / size_GiB) * 100, round_precision)
              << "%" << std::endl;

    size_t space_remapping_byte = rowCount * (32 + 32 + 5) / 8;
    std::cout << "-- Upper bound space remapping: " << space_remapping_byte
              << " bytes. "
              << round_to(double(space_remapping_byte) / GiB, round_precision)
              << " GiB." << std::endl;
    std::cout << "-- Loading time: " << loading_time << " seconds" << std::endl
              << std::flush;
    std::cout << "-- Loading speed " << (total_sst_size / MiB) / loading_time
              << " MiB/s" << std::endl
              << std::flush;

    std::vector<size_t> queries(rowCount);
    std::iota(queries.begin(), queries.end(), 0);
    std::mt19937 g2(21);
    std::shuffle(queries.begin(), queries.end(), g2);
    queries.resize(rowCount / div_num_queries);
    double full_random_access_speed = test_random_access(db, queries);

    std::cout << "-- Random access speed: " << full_random_access_speed
              << " MiB/s" << std::endl
              << std::flush;

    delete db;

    std::string to_run_remove = "rm -rf " + kDBPath;
    int status = system(to_run_remove.c_str());
    if (status != 0) {
        std::cout << "Failed to run " << to_run_remove << std::endl;
        exit(EXIT_FAILURE);
    }
    // std::cout << "DB closed" << std::endl;
}

// /disk2/data/25GiB/Python_selection/Python_selection_info_filename_len.csv
int main(int argc, char **argv) {
    if (argc <= 1) {
        usage(argv);
        exit(EXIT_FAILURE);
    }

    auto now =
            std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << "Start computation at " << std::ctime(&now);
    // std::cout << "Num threads: " << std::to_string(NUM_THREAD) << std::endl;
    std::cout << "Max num opened files " << exec("ulimit -n") << std::endl;

    for (int h = 1; h < argc; ++h) {
        std::string filename_path(argv[h]);
        std::cout << "filename_path " << filename_path << std::endl;
        std::size_t found = filename_path.find_last_of('/');
        // extract just the filename
        std::string filename = filename_path.substr(found + 1);

        const my_dataframe df(filename_path);

        if (filename == "C_small_filename_len.csv") {
            assert(df.get_num_files() == 7682);
        }

        //    std::function by_name = [](const my_dataframe &_df, size_t i) {
        //      return _df.filename_at(i) + std::to_string(i);
        //    };
        //
        //    std::function by_sha = [](const my_dataframe &_df, size_t i) {
        //      return _df.sha1_at_str(i);
        //    };

        //    std::function return_index = [](const my_dataframe &_df, size_t i) {
        //      return std::to_string(i);
        //    };

        const size_t rowCount = df.get_num_files();
        std::vector<size_t> random_permu(rowCount);

        std::iota(random_permu.begin(), random_permu.end(), 0);

        std::random_device rd;
        std::mt19937 g1(42);
        // std::mt19937 g2(21);

        std::shuffle(random_permu.begin(), random_permu.end(), g1);

        std::vector<std::string> compressors = {"kNoCompression",
                //"kSnappyCompression",
                // "kZlibCompression",
                // "kBZip2Compression",
                // "kLZ4Compression",
                // "kLZ4HCCompression",
                //"kXpressCompression",
                                                "kZSTD"};

        std::vector<std::string> compressors_multiple = {"kNoCompression",
                //"kSnappyCompression",
                // "kZlibCompression",
                // "kBZip2Compression",
                // "kLZ4Compression",
                // "kLZ4HCCompression",
                //"kXpressCompression",
                                                         "kZSTD", "kZSTD", "kZSTD"};

        std::vector<int> compressors_flags = {0, 3, 12, 22};

        // block size in bytes, default is 4KiB
        // 1MiB, 10MiB, 100MiB, 1GiB
        // std::vector<size_t> block_sizes = {
        //     1024 * 1024};  //, 100 * 1024 * 1024, 1024 * 1024 * 1024};

        std::vector<size_t> block_sizes = {
                size_t(2 * MiB),
                size_t(8 * MiB)  //,
        };  // 32 * 1024 * 1024};  //, 100 * 1024 * 1024, 1024 * 1024 * 1024};

        std::vector<size_t> sst_sizes = {size_t(2 * MiB), size_t(16 * MiB)};

        for (size_t i = 0; i < compressors_multiple.size(); ++i) {
            for (auto bs : block_sizes) {
                {
                    assert(check_is_permutation(random_permu));
                    load_in_rocksDB(df, random_permu, "random_permutation_index_keys",
                                    filename, compressors_multiple[i],
                                    compressors_flags[i], bs);
                }
                if (compressors_multiple[i] != "kNoCompression") {
                    std::vector<size_t> filename_ordered_rows(filename_sort(df));
                    assert(check_is_permutation(filename_ordered_rows));
                    load_in_rocksDB(df, filename_ordered_rows,
                                    "filename_as_keys_and_order_index_keys", filename,
                                    compressors_multiple[i], compressors_flags[i], bs);
                }
            }
        }

        for (size_t i = 0; i < compressors_multiple.size(); ++i) {
            for (auto ss : sst_sizes) {
                {
                    assert(check_is_permutation(random_permu));
                    bulk_load_in_rocksDB(df, random_permu,
                                         "random_permutation_index_keys", filename, ss,
                                         compressors_multiple[i], compressors_flags[i]);
                }
                if (compressors_multiple[i] != "kNoCompression") {
                    std::vector<size_t> filename_ordered_rows(filename_sort(df));
                    assert(check_is_permutation(filename_ordered_rows));
                    bulk_load_in_rocksDB(df, filename_ordered_rows,
                                         "filename_as_keys_and_order_index_keys",
                                         filename, ss, compressors_multiple[i],
                                         compressors_flags[i]);
                }
            }
        }
    }
    auto end =
            std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << "End computation at " << std::ctime(&end) << std::endl;
    return 0;
}