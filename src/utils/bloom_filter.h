#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

namespace LSMT {

class BloomFilter {
public:
    BloomFilter();

    BloomFilter(size_t expected_elements, double false_positive_rate);

    void add(const std::string &key);

    bool possibly_contain(const std::string &key) const;

    void clear();

    std::vector<uint8_t> encode();

    static BloomFilter decode(const std::vector<uint8_t> &data);

private:
    size_t hash(const std::string &key, size_t idx) const;

private:
    size_t bits_number;
    size_t hash_number;
    std::vector<bool> bits;
};
} // LOG STRUCTURED MERGE TREE