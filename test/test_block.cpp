#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <iomanip>
#include <memory>
#include <vector>

#include "../src/block/block.h"
#include "../src/block/block_iterator.h"

using namespace ::LSMT;

class BlockTest : public ::testing::Test {
protected:
std::vector<uint8_t> GetEncodedBlock() {
    std::vector<uint8_t> encoded = {
        // Data Section
        5, 0,                              // key_len = 5
        'a', 'p', 'p', 'l', 'e',           // key
        3, 0,                              // value_len = 3
        'r', 'e', 'd',                     // value
        1, 0, 0, 0, 0, 0, 0, 0,            // tranc_id = 1

        6, 0,                              // key_len = 6
        'b', 'a', 'n', 'a', 'n', 'a',      // key
        6, 0,                              // value_len = 6
        'y', 'e', 'l', 'l', 'o', 'w',      // value
        2, 0, 0, 0, 0, 0, 0, 0,            // tranc_id = 2

        6, 0,                              // key_len = 6
        'o', 'r', 'a', 'n', 'g', 'e',      // key
        7, 0,                              // value_len = 6
        'o', 'r', 'a', 'n', 'g', 'e', '3', // value
        3, 0, 0, 0, 0, 0, 0, 0,            // tranc_id = 3

        6, 0,                              // key_len = 6
        'o', 'r', 'a', 'n', 'g', 'e',      // key
        7, 0,                              // value_len = 6
        'o', 'r', 'a', 'n', 'g', 'e', '2', // value
        2, 0, 0, 0, 0, 0, 0, 0,            // tranc_id = 2

        6, 0,                              // key_len = 6
        'o', 'r', 'a', 'n', 'g', 'e',      // key
        7, 0,                              // value_len = 6
        'o', 'r', 'a', 'n', 'g', 'e', '1', // value
        1, 0, 0, 0, 0, 0, 0, 0,            // tranc_id = 1
        
        // Offset Section
        0,  0,  // offset[0] = 0
        20, 0,  // offset[1] = 20
        44, 0,  // offset[2] = 44
        69, 0,  // offset[3] = 69
        94, 0,  // offset[4] = 94

        // Elements Number
        5, 0,
    };
    return encoded;
}
};

TEST_F(BlockTest, DecodeTest) {
    std::vector<uint8_t> encoded = GetEncodedBlock();
    auto block = Block::decode(encoded, false);

    EXPECT_EQ(block->get_first_key(), "apple");

    EXPECT_EQ(block->get_val_binary("apple", 0).value(), "red");

    EXPECT_EQ(block->get_val_binary("banana", 0).value(), "yellow");

    EXPECT_EQ(block->get_val_binary("orange", 1).value(), "orange1");
    EXPECT_EQ(block->get_val_binary("orange", 2).value(), "orange2");
    EXPECT_EQ(block->get_val_binary("orange", 3).value(), "orange3");
}

TEST_F(BlockTest, EncodeTest) {
    Block block(1024);
    block.add_entry("apple", "red", 1, false);
    block.add_entry("banana", "yellow", 2, false);
    block.add_entry("orange", "orange3", 3, false);
    block.add_entry("orange", "orange2", 2, false);
    block.add_entry("orange", "orange1", 1, false);

    std::vector<uint8_t> encoded = block.encode();

    auto decoded = Block::decode(encoded);
    EXPECT_EQ(decoded->get_val_binary("apple", 1).value(), "red");
    EXPECT_EQ(decoded->get_val_binary("banana", 2).value(), "yellow");
    EXPECT_EQ(decoded->get_val_binary("orange", 0).value(), "orange3");

    EXPECT_EQ(decoded->get_val_binary("orange", 3).value(), "orange3");
    EXPECT_EQ(decoded->get_val_binary("orange", 2).value(), "orange2");
    EXPECT_EQ(decoded->get_val_binary("orange", 1).value(), "orange1");
}

TEST_F(BlockTest, BinarySearchTest) {
    Block block(1024);
    block.add_entry("apple", "red", 0, false);
    block.add_entry("banana", "yellow", 0, false);
    block.add_entry("orange", "orange", 0, false);

    EXPECT_EQ(block.get_val_binary("apple", 0).value(), "red");
    EXPECT_EQ(block.get_val_binary("banana", 0).value(), "yellow");
    EXPECT_EQ(block.get_val_binary("orange", 0).value(), "orange");

    EXPECT_FALSE(block.get_val_binary("watermelon", 0).has_value());
}

TEST_F(BlockTest, EdgeCaseTest) {
    Block block(1024);

    EXPECT_EQ(block.get_first_key(), "");
    EXPECT_EQ(block.get_val_binary("any", 0).has_value(), false);

    block.add_entry("", "", 0, false);
    EXPECT_EQ(block.get_first_key(), "");
    EXPECT_EQ(block.get_val_binary("", 0).value(), "");

    block.add_entry("key\0with\tnull", "value\rwith\nnull", 0, false);
    EXPECT_EQ(block.get_val_binary("key\0with\tnull", 0), "value\rwith\nnull");
}

TEST_F(BlockTest, LargeDataTest) {
    Block block(1024 * 32);

    for (int i = 0; i < 1000; ++i) {
        char key_buf[16], val_buf[16];
        snprintf(key_buf, sizeof(key_buf), "key%03d", i);
        snprintf(val_buf, sizeof(val_buf), "val%03d", i);
        block.add_entry(std::string(key_buf), std::string(val_buf), 0, false);
    }
    for (int i = 0; i < 1000; ++i) {
        char key_buf[16], val_buf[16];
        snprintf(key_buf, sizeof(key_buf), "key%03d", i);
        snprintf(val_buf, sizeof(val_buf), "val%03d", i);
        EXPECT_EQ(block.get_val_binary(std::string(key_buf), 0), std::string(val_buf));
    }
}

TEST_F(BlockTest, ErrorHandlingTest) {
    std::vector<uint8_t> error_data = {1}, empty_data;
    EXPECT_THROW(Block::decode(error_data), std::runtime_error);
    EXPECT_THROW(Block::decode(empty_data), std::runtime_error);
}

TEST_F(BlockTest, IteratorTest1) {
    auto block = std::make_shared<Block>(4096);

    EXPECT_EQ(block->begin(), block->end());

    std::vector<std::pair<std::string, std::string>> kvpairs;
    for (int i = 0; i < 100; ++i) {
        char key_buf[16], val_buf[16];
        snprintf(key_buf, sizeof(key_buf), "key%03d", i);
        snprintf(val_buf, sizeof(val_buf), "val%03d", i);
        block->add_entry(std::string(key_buf), std::string(val_buf), 0, false);
        kvpairs.emplace_back(std::string(key_buf), std::string(val_buf));
    }

    size_t count = 0;
    for (const auto &[key, val] : *block) {
        EXPECT_EQ(key, kvpairs[count].first);
        EXPECT_EQ(val, kvpairs[count].second);
        count++;
    }
    EXPECT_EQ(count, kvpairs.size());

    BlockIterator it = block->begin();
    EXPECT_EQ(it->first, "key000");
    ++it;
    EXPECT_EQ(it->first, "key001");
    ++it;
    EXPECT_EQ(it->first, "key002");
}

TEST_F(BlockTest, IteratorTest2) {
    auto block = std::make_shared<Block>(4096);

    block->add_entry("key1", "value11", 1, false);
    block->add_entry("key2", "value23", 3, false);
    block->add_entry("key2", "value22", 2, false);
    block->add_entry("key2", "value21", 1, false);
    block->add_entry("key3", "value31", 1, false);
    block->add_entry("key4", "value41", 1, false);

    std::vector<std::pair<std::string, std::string>> kvpairs = {
        {"key1", "value11"},
        {"key2", "value23"},
        {"key3", "value31"},
        {"key4", "value41"},
    };

    std::vector<std::pair<std::string, std::string>> results;

    for (auto it = block->begin(); it != block->end(); ++it) {
        results.emplace_back(it->first, it->second);
    }
    EXPECT_EQ(kvpairs, results);
}

TEST_F(BlockTest, PredicateTest1) {
    std::shared_ptr<Block> block = std::make_shared<Block>(4096);
    int number = 50;
    for (int i = 0; i < 50; ++i) {
        std::ostringstream oss_key;
        std::ostringstream oss_val;
        oss_key << "key" << std::setw(4) << std::setfill('0') << i;
        oss_val << "val" << std::setw(4) << std::setfill('0') << i;
        block->add_entry(oss_key.str(), oss_val.str(), 0, false);
    }
    auto result = block->get_monotony_predicate_iters(0, [](const std::string &key) {
        return (key < "key0020") - (key >= "key0030");
    });
    EXPECT_TRUE(result.has_value());
    auto [it_beg, it_end] = result.value();
    EXPECT_EQ((*it_beg)->first, "key0020");
    EXPECT_EQ((*it_end)->first, "key0030");
}

TEST_F(BlockTest, PredicateTest2) {
    std::shared_ptr<Block> block = std::make_shared<Block>(4096);

    block->add_entry("key0", "value00", 0, false);
    block->add_entry("key1", "value11", 1, false);
    block->add_entry("key2", "value29", 9, false);
    block->add_entry("key2", "value22", 2, false);
    block->add_entry("key3", "value32", 3, false);
    block->add_entry("key4", "value49", 9, false);
    block->add_entry("key4", "value48", 8, false);
    block->add_entry("key4", "value47", 7, false);
    block->add_entry("key4", "value44", 4, false);
    block->add_entry("key5", "value58", 8, false);
    block->add_entry("key5", "value57", 7, false);
    block->add_entry("key5", "value56", 6, false);
    block->add_entry("key5", "value55", 5, false);
    block->add_entry("key6", "value66", 6, false);

    auto result = block->get_monotony_predicate_iters(7, [](const std::string &key) {
        return (key < "key2") - (key >= "key6");
    });
    EXPECT_TRUE(result.has_value());
    auto [it_beg, it_end] = result.value();
    std::vector<std::pair<std::string, std::string>> results;
    for (auto it = it_beg; *it != *it_end; ++(*it)) {
        results.emplace_back((*it)->first, (*it)->second);
    }
    std::vector<std::pair<std::string, std::string>> kvpairs = {
        {"key2", "value22"},
        {"key3", "value32"},
        {"key4", "value47"},
        {"key5", "value57"},
    };
    EXPECT_EQ(kvpairs, results);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}