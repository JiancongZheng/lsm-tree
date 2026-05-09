#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <optional>
#include <random>
#include <unordered_map>
#include <iostream>

#include "lsm/lsm_engine.h"
#include "lsm/lsm_iterator.h"

using namespace ::LSMT;

class LSMTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_path = "test_lsm_path";
        if (std::filesystem::exists(test_path)) {
            std::filesystem::remove_all(test_path);
        }
        std::filesystem::create_directory(test_path);
    }

    void TearDown() override {
        if (!no_clear && std::filesystem::exists(test_path)) {
            std::filesystem::remove_all(test_path);
        }
    }

    void set_clear(bool not_clear) {
        no_clear = not_clear;
    }

    std::string test_path;
    bool no_clear = false;
};

TEST_F(LSMTest, BasicOperations) {
    LSMTree lsm_tree(test_path);

    lsm_tree.put("key1", "old_value1");
    EXPECT_EQ(lsm_tree.get("key1").value(), "old_value1");
    lsm_tree.put("key2", "old_value2");
    EXPECT_EQ(lsm_tree.get("key2").value(), "old_value2");
    lsm_tree.put("key3", "old_value3");
    EXPECT_EQ(lsm_tree.get("key3").value(), "old_value3");
    lsm_tree.put("key4", "old_value4");
    EXPECT_EQ(lsm_tree.get("key4").value(), "old_value4");

    lsm_tree.put("key1", "new_value1");
    EXPECT_EQ(lsm_tree.get("key1").value(), "new_value1");
    lsm_tree.put("key3", "new_value3");
    EXPECT_EQ(lsm_tree.get("key3").value(), "new_value3");

    lsm_tree.remove("key2");
    EXPECT_FALSE(lsm_tree.get("key2").has_value());
    lsm_tree.remove("key4");
    EXPECT_FALSE(lsm_tree.get("key4").has_value());

    EXPECT_FALSE(lsm_tree.get("NO_EXIST").has_value());
}

TEST_F(LSMTest, LargeScaleOperations) {
    LSMTree lsm_tree(test_path);
    std::vector<std::pair<std::string, std::string>> expected;

    for (int i = 0; i < 100000; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string val = "val" + std::to_string(i);
        lsm_tree.put(key, val);
        expected.emplace_back(key, val);
    }

    for (const auto &[key, value] : expected) {
        EXPECT_EQ(lsm_tree.get(key).value(), value);
    }
}

TEST_F(LSMTest, IteratorOperations) {
    LSMTree lsm_tree(test_path);
    std::map<std::string, std::string> expected;

    for (int i = 0; i < 10000; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string val = "val" + std::to_string(i);
        expected[key] = val;
        lsm_tree.put(key, val);
    }

    auto lsm_iter = lsm_tree.begin(0);
    auto exp_iter = expected.begin();

    while (lsm_iter != lsm_tree.end() && exp_iter != expected.end()) {
        EXPECT_EQ(lsm_iter->first, exp_iter->first);
        EXPECT_EQ(lsm_iter->second, exp_iter->second);
        ++lsm_iter;
        ++exp_iter;
    }
    EXPECT_EQ(lsm_iter, lsm_tree.end());
    EXPECT_EQ(exp_iter, expected.end());
}

TEST_F(LSMTest, MixedOperations) {
    LSMTree lsm_tree(test_path);
    std::unordered_map<std::string, std::optional<std::string>> expected;
    std::mt19937 gen(2021);
    std::uniform_int_distribution<int> key_dist(0, 5000);

    for (int i = 0; i < 10000; ++i) {
        int index = key_dist(gen);
        std::string key = "key" + std::to_string(index);
        std::string val = "val" + std::to_string(index);
        lsm_tree.put(key, val);
        expected[key] = val;
        if (index % 4 == 0) {
            lsm_tree.remove(key);
            expected[key] = std::nullopt;
        }        
    }

    for (const auto &[key, value] : expected) {
        auto result = lsm_tree.get(key);
        if (value.has_value()) {
            ASSERT_TRUE(result.has_value());
            EXPECT_EQ(result.value(), value.value());
        } else {
            EXPECT_FALSE(result.has_value());
        }
    }
}

TEST_F(LSMTest, Persistence) {
    std::unordered_map<std::string, std::optional<std::string>> expected;

    {   // Insert data and flush to disk and destroy LSM instance
        LSMTree lsm_tree(test_path);
        for (int i = 0; i < 100000; ++i) {
            std::string key = "key" + std::to_string(i);
            std::string val = "val" + std::to_string(i);
            lsm_tree.put(key, val);
            expected[key] = val;

            if (i % 10 == 0 && i != 0) {
                lsm_tree.remove(key);
                expected[key] = std::nullopt;
            }
        }
        set_clear(true);
    }
    
    {   // Create new LSM instance to verify data persistence
        LSMTree lsm_tree(test_path);
        for (int i = 0; i < 100000; ++i) {
            std::string key = "key" + std::to_string(i);
            if (expected[key].has_value()) {
                ASSERT_TRUE(lsm_tree.get(key).has_value());
                EXPECT_EQ(lsm_tree.get(key).value(), expected[key]);
            } else {
                EXPECT_FALSE(lsm_tree.get(key).has_value());
            }
        }
        set_clear(false);
    }
}

TEST_F(LSMTest, MonotonyPredicate) {
    LSMTree lsm_tree(test_path);
    std::set<std::string> expect_keys;
    std::set<std::string> actual_keys;

    for (int i = 0; i < 100; ++i) {
        std::ostringstream oss_key;
        std::ostringstream oss_val;
        oss_key << "key" << std::setw(2) << std::setfill('0') << i;
        oss_val << "val" << std::setw(2) << std::setfill('0') << i;
        lsm_tree.put(oss_key.str(), oss_val.str());
        if (i == 100) {
            lsm_tree.flush_one();
        }
    }

    auto predicate = [](const std::string &key) -> int {
        if (key < "key20") return 1;
        else if (key > "key60") return -1;
        else return 0;
    };

    auto result = lsm_tree.iter_monotony_predicate(0, predicate);

    ASSERT_TRUE(result.has_value());
    for (int i = 20; i <= 60; ++i) {
        std::ostringstream oss;
        oss << "key" << std::setw(2) << std::setfill('0') << i;
        expect_keys.insert(oss.str());
    }
    auto [begin, end] = result.value();
    for (auto it = begin; it != end; ++it) {
        actual_keys.insert(it->first);
    }
    EXPECT_EQ(expect_keys, actual_keys);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
