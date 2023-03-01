//
// Created by boffa on 01/03/23.
//

#ifndef SWH_COMPRESSOR_MY_DATAFRAME_H
#define SWH_COMPRESSOR_MY_DATAFRAME_H

#include <sstream>
#include <iomanip>
#include <boost/compute/detail/sha1.hpp>
#include <vector>
#include <fstream>

class my_dataframe {
private:
    // should be std::array<std::byte, 20>
    std::vector<std::array<unsigned char, 40>> sha1s;
    std::vector<size_t> lengths;
    std::vector<std::string> filenames;
    size_t num_files;

public:
    explicit my_dataframe(std::string &path, size_t _num_files) {
        sha1s.reserve(num_files);
        lengths.reserve(num_files);
        filenames.reserve(num_files);

        num_files = _num_files;

        std::ifstream source(path, std::ios_base::binary);
        if (source.is_open()) {
            char c;
            // skipping header
            while (c != '\n')
                source.read(&c, 1);
            //21905e249f3d8411e1d84686f1cecaa2c633b981
            for (auto h = 0; h < num_files; h++) {
                std::array<unsigned char, 40> tmp_sha1{};
                for (auto i = 0; i < 40; i++) {
                    source.read(&c, 1);
                    tmp_sha1[i] = c;
                }
                sha1s.push_back(tmp_sha1);
                //skip comma
                source.read(&c, 1);
                size_t len;
                size_t filename_size;
                source >> len;
                lengths.push_back(len);
                // skip comma
                source.read(&c, 1);
                source >> filename_size;
                // skip comma
                source.read(&c, 1);
                std::vector<char> tmp(filename_size);
                source.read(tmp.data(), filename_size);
                filenames.emplace_back(tmp.begin(), tmp.end());
                // skip \n
                source.read(&c, 1);
            }
        } else {
            std::cout << "Error opening file";
        }
    }

    std::string filename_at(size_t idx) {
        return filenames[idx];
    }

    std::array<unsigned char, 40> sha1_at(size_t idx) {
        return sha1s[idx];
    }

    std::string sha1_at_str(size_t idx) {
        std::array<unsigned char, 40> a(sha1_at(idx));
        std::string to_return;
        for (auto i = 0; i < 40; i++) {
            to_return[i] = a[i];
        }
        return to_return;
    }

    size_t get_num_files(){
        return num_files;
    }
};

#endif //SWH_COMPRESSOR_MY_DATAFRAME_H
