#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "config/config.h"
#include "sst/sst.h"
#include "sst/sst_builder.h"
#include "sst/sst_iterator.h"

using namespace ::LSMT;

class SSTTest : public ::testing::Test
{
protected:
    void SetUp() override {
        if (std::filesystem::exists("test_sst_path") == false) {
            std::filesystem::create_directory("test_sst_path");
        }
    }

    void TearDown() override {
        std::filesystem::remove_all("test_sst_path");
    }

    std::shared_ptr<SST> create_test_sst(size_t block_size, size_t num_entries) {
        SSTBuilder builder(block_size, true);

        for (size_t i = 0; i < num_entries; i++) {
            std::string key = "key" + std::to_string(i);
            std::string val = "val" + std::to_string(i);
            builder.add(key, val, 0);
        }

        auto block_cache = std::make_shared<BlockCache>(
            TomlConfig::get_instance().get_lsm_block_cache_size(),
            TomlConfig::get_instance().get_lsm_block_cache_lruk());

        return builder.build(1, "test_sst_path/test_sst0", block_cache);
    }
};

TEST_F(SSTTest, BasicWriteAndRead) {
    SSTBuilder builder(1024, true);
    auto block_cache = std::make_shared<BlockCache>(
        TomlConfig::get_instance().get_lsm_block_cache_size(),
        TomlConfig::get_instance().get_lsm_block_cache_lruk());

    builder.add("key1", "value1", 0);
    builder.add("key2", "value2", 0);
    builder.add("key3", "value3", 0);

    auto sst = builder.build(1, "test_sst_path/test_sst1", block_cache);

    EXPECT_EQ(sst->get_fkey(), "key1");
    EXPECT_EQ(sst->get_lkey(), "key3");
    EXPECT_EQ(sst->get_sst_id(), 1);

    auto block = sst->get_block(0);
    EXPECT_TRUE(block != nullptr);
    auto value = block->get_val_binary("key2", 0);
    EXPECT_TRUE(value.has_value());
}

TEST_F(SSTTest, BlockSplitting) {
    SSTBuilder builder(64, true);
    auto block_cache = std::make_shared<BlockCache>(
        TomlConfig::get_instance().get_lsm_block_cache_size(),
        TomlConfig::get_instance().get_lsm_block_cache_lruk());

    for (int i = 0; i < 10; i++) {
        std::string key = "key" + std::to_string(i);
        std::string val = "key" + std::string(20, i);
        builder.add(key, val, 0);
    }

    auto sst = builder.build(1, "test_sst_path/test_sst2", block_cache);

    for (size_t i = 0; i < sst->get_block_number(); i++) {
        auto block = sst->get_block(i);
        EXPECT_TRUE(block != nullptr);
    }
}

TEST_F(SSTTest, KeySearch) {
    auto sst = create_test_sst(256, 100);

    auto index = sst->get_block_id("key50");
    auto block = sst->get_block(index);
    auto value = block->get_val_binary("key50", 0);
    EXPECT_TRUE(value.has_value());
    EXPECT_TRUE(value.value() == "val50");

    EXPECT_TRUE(sst->get_block_id("key999") == -1);
}

TEST_F(SSTTest, EmptySST) {
    SSTBuilder builder(1024, true);
    auto block_cache = std::make_shared<BlockCache>(
        TomlConfig::get_instance().get_lsm_block_cache_size(),
        TomlConfig::get_instance().get_lsm_block_cache_lruk());
    EXPECT_THROW(builder.build(1, "test_sst_path/test_sst3", block_cache), std::runtime_error);
}

TEST_F(SSTTest, ReopenSST) {
    auto sst = create_test_sst(256, 10);
    auto block_cache = std::make_shared<BlockCache>(
        TomlConfig::get_instance().get_lsm_block_cache_size(),
        TomlConfig::get_instance().get_lsm_block_cache_lruk());

    FileObj file = FileObj::open("test_sst_path/test_sst0", false);
    auto new_sst = SST::open(1, std::move(file), block_cache);

    EXPECT_EQ(sst->get_fkey(), new_sst->get_fkey());
    EXPECT_EQ(sst->get_lkey(), new_sst->get_lkey());
    EXPECT_EQ(sst->get_block_number(), new_sst->get_block_number());
}

TEST_F(SSTTest, LargeSST) {
    SSTBuilder builder(4096, true);
    auto block_cache = std::make_shared<BlockCache>(
        TomlConfig::get_instance().get_lsm_block_cache_size(),
        TomlConfig::get_instance().get_lsm_block_cache_lruk());

    for (int i = 0; i < 1000; i++) {
        // 键格式：key000, key001, ..., key999
        // 值格式：val000, val001, ..., val999
        std::string key = "key" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
        std::string val = "val" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
        builder.add(key, val, 0);
    }

    auto sst = builder.build(1, "test_sst_path/test_sst4", block_cache);

    EXPECT_GT(sst->get_block_number(), 1);
    EXPECT_EQ(sst->get_fkey(), "key000");
    EXPECT_EQ(sst->get_lkey(), "key999");

    std::vector<int> offsets = {0, 100, 500, 999};
    for (int i : offsets) {
        std::string key = "key" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
        auto index = sst->get_block_id(key);
        auto block = sst->get_block(index);
        auto value = block->get_val_binary(key, 0);
        EXPECT_TRUE(value.has_value());

        std::string exp_value = "val" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
        EXPECT_TRUE(value.value() == exp_value);
    }
}

TEST_F(SSTTest, LargeSSTPredicate) {
    SSTBuilder builder(4096, true);
    auto block_cache = std::make_shared<BlockCache>(
        TomlConfig::get_instance().get_lsm_block_cache_size(),
        TomlConfig::get_instance().get_lsm_block_cache_lruk());

    for (int i = 0; i < 1000; i++) {
        // 键格式：key000, key001, ..., key999
        // 值格式：val000, val001, ..., val999
        std::string key = "key" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
        std::string val = "val" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
        builder.add(key, val, 0);
    }

    auto sst = builder.build(1, "test_sst_path/test_sst5", block_cache);

    auto result = sst->iters_monotony_predicate(0, [](const std::string &key) {
        return key.compare("key300") < 0 ? 1 : (key.compare("key500") > 0 ? -1 : 0);
    });

    EXPECT_TRUE(result.has_value());
    auto [iter_beg, iter_end] = result.value();
    EXPECT_TRUE(iter_beg.get_key() == "key300");
    EXPECT_TRUE(iter_end.get_key() == "key501");
    for (int i = 0; i <= 200; ++i) {
        EXPECT_TRUE(iter_beg.get_key() == "key" + std::to_string(i + 300));
        EXPECT_TRUE(iter_beg.get_val() == "val" + std::to_string(i + 300));
        ++iter_beg;
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}