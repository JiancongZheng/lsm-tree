#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <filesystem>
#include <random>

#include "config/config.h"
#include "utils/bloom_filter.h"
#include "utils/files.h"

using namespace ::LSMT;

class FileTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists("file_test")) {
            std::filesystem::create_directory("file_test");
        }
    }

    void TearDown() override {
        // std::filesystem::remove_all("file_test");
    }

    std::vector<uint8_t> generate_random_data(size_t size)
    {
        std::vector<uint8_t> data(size);

        for (size_t i = 0; i < size; ++i) {
            data[i] = static_cast<uint8_t>(random() % 256);
        }
        return data;
    }
};

TEST_F(FileTest, BasicReadAndWrite) {
    const std::string path = "file_test/basic.dat";
    std::vector<uint8_t> data = {1, 2, 3, 4, 5, 6};

    auto raw_file = FileObj::create_and_write(path, data);
    EXPECT_EQ(raw_file.size(), data.size());

    auto new_file = FileObj::open(path, false);
    EXPECT_EQ(new_file.size(), data.size());

    auto read_data = new_file.read(0, data.size());
    EXPECT_EQ(read_data, data);
}

TEST_F(FileTest, LargeReadAndWrite) {
    const std::string path = "file_test/large.dat";
    const size_t data_size = 1024 * 1024;
    auto data = generate_random_data(data_size);

    auto raw_file = FileObj::create_and_write(path, data);
    EXPECT_EQ(raw_file.size(), data_size);

    auto new_file = FileObj::open(path, false);
    
    for (size_t offset = 0; offset < data_size; offset += 1024) {
        auto raw_data = std::vector<uint8_t>(data.begin() + offset, data.begin() + offset + 1024);
        auto new_data = new_file.read(offset, 1024);
        EXPECT_EQ(raw_data, new_data);
    }
}

TEST_F(FileTest, PartialRead) {
    const std::string path = "file_test/partial.dat";
    std::vector<uint8_t> data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    auto raw_file = FileObj::create_and_write(path, data);
    EXPECT_EQ(raw_file.size(), data.size());

    auto new_file = FileObj::open(path, false);

    auto beg_data = new_file.read(0, 2);
    EXPECT_EQ(beg_data[0], 1);
    EXPECT_EQ(beg_data[1], 2);

    auto mid_data = new_file.read(2, 3);
    EXPECT_EQ(mid_data[0], 3);
    EXPECT_EQ(mid_data[1], 4);
    EXPECT_EQ(mid_data[2], 5);

    auto end_data = new_file.read(8, 2);
    EXPECT_EQ(end_data[0], 9);
    EXPECT_EQ(end_data[1], 10);
}

// 测试错误情况
TEST_F(FileTest, ErrorCases) {
    const std::string path = "file_test/error.dat";
    std::vector<uint8_t> data = {1, 2, 3};

    auto raw_file = FileObj::create_and_write(path, data);
    auto new_file = FileObj::open(path, false);

    EXPECT_THROW(new_file.read(2, 2), std::out_of_range);
    EXPECT_THROW(new_file.read(3, 1), std::out_of_range);
    EXPECT_THROW(new_file.read(0, 4), std::out_of_range);

    EXPECT_THROW(FileObj::open("nonexistent.dat", false), std::runtime_error);
}

TEST(BloomFilterTest, BloomFilterOperation) {
    BloomFilter filter(1000, 0.1);

    for (int i = 0; i < 1000; ++i) {
        filter.add("bloom_filter" + std::to_string(i));
    }

    for (int i = 0; i < 1000; ++i) {
        EXPECT_TRUE(filter.possibly_contain("bloom_filter" + std::to_string(i)))
            << "Key bloom_filter" << i << " should be found in the BloomFilter";
    }

    int false_positive = 0;
    for (int i = 1000; i < 2000; ++i) {
        if (filter.possibly_contain("key" + std::to_string(i))) {
            false_positive++;
        }
    }
    double false_positive_rate = static_cast<double>(false_positive) / 1000;
    EXPECT_LE(false_positive_rate, 0.2) << "False positive rate " << false_positive_rate;
}

TEST(TomlConfigTest, TomleConfigOperation) {
    TomlConfig config = TomlConfig::get_instance("../config.toml");

    EXPECT_EQ(config.get_lsm_sum_memtable_size(), 1024 * 1024 * 64);
    EXPECT_EQ(config.get_lsm_per_memtable_size(), 1024 * 1024 * 4);
    EXPECT_EQ(config.get_lsm_sst_level_ratio(), 4);
    EXPECT_EQ(config.get_lsm_block_size(), 32768);
    EXPECT_EQ(config.get_lsm_block_cache_size(), 1024);
    EXPECT_EQ(config.get_lsm_block_cache_lruk(), 8);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
