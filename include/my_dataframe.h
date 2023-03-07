//
// Created by boffa on 01/03/23.
//
#pragma once

#include <sstream>
#include <iomanip>
#include <boost/compute/detail/sha1.hpp>
#include <vector>
#include <fstream>
#include <cassert>
#include <filesystem>

//#include "utils.hpp"

class my_dataframe {
private:
    // to save space should be std::array<std::byte, 20>
    std::vector<std::array<unsigned char, 40>> sha1s;
    std::vector<size_t> lengths;
    std::vector<std::string> filenames;

    size_t num_files = 0;
    size_t total_size = 0;

public:
    explicit my_dataframe(std::string &path, size_t _num_files) {
        num_files = _num_files;
        sha1s.reserve(num_files);
        lengths.reserve(num_files);
        filenames.reserve(num_files);

        std::ifstream source(path, std::ios_base::binary);
        if (source.is_open()) {
            char c;
            // skipping header
            while (c != '\n')
                source.read(&c, 1);
            // example of SHA1
            //21905e249f3d8411e1d84686f1cecaa2c633b981
            for (size_t h = 0; h < num_files and !source.eof(); h++) {
                std::array<unsigned char, 40> tmp_sha1{};
                for (auto i = 0; i < 40; i++) {
                    source.read(&c, 1);
                    tmp_sha1[i] = c;
                }
                for (auto ch: tmp_sha1) {
                    assert(isxdigit(ch));
                }
                sha1s.push_back(tmp_sha1);
                //skip comma
                source.read(&c, 1);
                assert(c == ',');
                std::string num;
                //std::cout << source.tellg() << std::endl;
                getline(source, num, ',');
                //std::cout << source.tellg() << std::endl;

                size_t len = std::strtoul(num.c_str(), nullptr, 10);
                lengths.push_back(len);
                total_size += len;

                getline(source, num, ',');
                //std::cout << source.tellg() << std::endl;
                size_t filename_size = std::strtoul(num.c_str(), nullptr, 10);

                std::vector<char> tmp(filename_size);
                source.read(tmp.data(), filename_size);
                filenames.emplace_back(tmp.begin(), tmp.end());
                // skip \n
                source.read(&c, 1);
                assert(c == '\n');
            }
            size_t total_size_from_fs = 0;
            size_t size_from_fs = 0;
            for (size_t i = 0; i < num_files; ++i) {
                std::string sha1 = this->sha1_at_str(i);
                std::filesystem::path blob_hash(sha1);
                // TODO change BLOBS_DIR
                std::string filename_path("/data/swh/blobs" / blob_hash);
                size_from_fs = std::filesystem::file_size(filename_path);
                total_size_from_fs += size_from_fs;
                assert(size_from_fs == this->length_at(i));
            }
            assert(total_size_from_fs == total_size);
        } else {
            std::cout << "Error opening file";
        }
    }

    size_t length_at(size_t idx) const {
        return lengths[idx];
    }

    std::string filename_at(size_t idx) const {
        return filenames[idx];
    }

    std::array<unsigned char, 40> sha1_at(size_t idx) const {
        return sha1s[idx];
    }

    std::string sha1_at_str(size_t idx) const {
        std::array<unsigned char, 40> a(sha1_at(idx));
        std::string to_return;
        to_return.resize(40);
        for (auto i = 0; i < 40; i++) {
            to_return[i] = char(a[i]);
        }
        return to_return;
    }

    size_t get_num_files() const {
        return num_files;
    }

    size_t get_total_size() const {
        return total_size;
    }
};
