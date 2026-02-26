#include "./bloom_filter.h"

namespace LSMT {
BloomFilter::BloomFilter() { }

BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate) {
    double m = -static_cast<double>(expected_elements) * std::log(false_positive_rate) / std::pow(std::log(2), 2);
    bits_number = static_cast<size_t>(std::ceil(m));
    hash_number = static_cast<size_t>(std::ceil(m / expected_elements * std::log(2)));
    bits.resize(bits_number, false);
}

void BloomFilter::add(const std::string &key) {
    for (size_t i = 0; i < hash_number; ++i) {
        bits[hash(key, i)] = true;
    }
}

bool BloomFilter::possibly_contain(const std::string &key) const {
    for (size_t i = 0; i < hash_number; ++i) {
        if (bits[hash(key, i)] == false) {
            return false;
        }
    }
    return true;
}

void BloomFilter::clear() {
    bits.assign(bits.size(), false);
}

size_t BloomFilter::hash(const std::string &key, size_t idx) const {
    return std::hash<std::string>()(key + std::to_string(idx));
}

std::vector<uint8_t> BloomFilter::encode() {
    std::vector<uint8_t> data;

    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&bits_number),
                reinterpret_cast<const uint8_t*>(&bits_number) + sizeof(bits_number));

    data.insert(data.end(), reinterpret_cast<const uint8_t*>(&hash_number),
                reinterpret_cast<const uint8_t*>(&hash_number) + sizeof(hash_number));

    for (size_t i = 0; i < (bits_number + 7) / 8; ++i) {
        uint8_t byte = 0;
        for (size_t j = 0; j < 8 && i * 8 + j < bits_number; ++j) {
            byte = byte | (bits[i * 8 + j] << j);
        }
        data.push_back(byte);
    }

    return data;
}

BloomFilter BloomFilter::decode(const std::vector<uint8_t> &data) {
    BloomFilter bf;
    size_t index = 0;

    std::memcpy(&bf.bits_number, &data[index], sizeof(bf.bits_number));
    index += sizeof(bf.bits_number);

    std::memcpy(&bf.hash_number, &data[index], sizeof(bf.hash_number));
    index += sizeof(bf.hash_number);

    bf.bits.resize(bf.bits_number, false);
    for (size_t i = 0; i < (bf.bits_number + 7) / 8; ++i) {
        uint8_t byte = data[index++];
        for (size_t j = 0; j < 8 && i * 8 + j < bf.bits_number; ++j) {
            bf.bits[i * 8 + j] = (byte >> j) & 1;
        }
    }

    return bf;
}
} // LOG STRUCTURED MERGE TREE
