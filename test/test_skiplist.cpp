#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <unordered_set>
#include <unordered_map>

#include "../src/skiplist/skiplist.h"

using namespace ::LSMT;

TEST(SkipListTest, BasicOperation) {
    SkipList skiplist;

    skiplist.put("key1", "old_value", 0);
    EXPECT_EQ(skiplist.get("key1", 0).get_val(), "old_value");

    skiplist.put("key1", "new_value", 0);
    EXPECT_EQ(skiplist.get("key1", 0).get_val(), "new_value");

    skiplist.remove("key1");
    EXPECT_FALSE(skiplist.get("key1", 0).is_valid());
}

TEST(SkipListTest, Iterator) {
    SkipList skiplist;
    skiplist.put("key1", "value1", 0);
    skiplist.put("key2", "value2", 0);
    skiplist.put("key3", "value3", 0);

    std::vector<std::pair<std::string, std::string>> result;
    for (auto it = skiplist.begin(); it != skiplist.end(); ++it) {
        result.push_back(*it);
    }

    EXPECT_EQ(result.size(), 3);
    EXPECT_EQ(std::get<0>(result[0]), "key1");
    EXPECT_EQ(std::get<0>(result[1]), "key2");
    EXPECT_EQ(std::get<0>(result[2]), "key3");
}

TEST(SkipListTest, DuplicateInsert) {
    SkipList skiplist;

    skiplist.put("key0", "value1", 0);
    skiplist.put("key0", "value2", 0);
    skiplist.put("key0", "value3", 0);

    EXPECT_EQ(skiplist.get("key0", 0).get_val(), "value3");
}

TEST(SkipListTest, SequentialInsertAndRemove) {
    SkipList skiplist;
    const int number = 10000;

    for (int i = 0; i < number; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string val = "val" + std::to_string(i);
        skiplist.put(key, val, 0);
    }
    for (int i = 0; i < number; ++i) {
        std::string key = "key" + std::to_string(i);
        skiplist.remove(key);
    }
    for (int i = 0; i < number; ++i) {
        std::string key = "key" + std::to_string(i);
        EXPECT_FALSE(skiplist.get(key, 0).is_valid());
    }
}

TEST(SkipListTest, MemorySizeTracking) {
    SkipList skiplist;

    skiplist.put("key1", "value1", 0);
    skiplist.put("key2", "value2", 0);
    size_t expect_size = sizeof("key1") - 1 + sizeof("value1") - 1 + sizeof(uint64_t) +
                         sizeof("key2") - 1 + sizeof("value2") - 1 + sizeof(uint64_t);
    EXPECT_EQ(skiplist.get_size(), expect_size); 
    
    skiplist.remove("key1");
    expect_size -= sizeof("key1") - 1 + sizeof("value1") - 1 + sizeof(uint64_t);
    EXPECT_EQ(skiplist.get_size(), expect_size);
}

TEST(SkipListTest, IteratorPreffix) {
    SkipList skiplist;

    skiplist.put("apple1", "value1", 0);
    skiplist.put("apple2", "value2", 0);
    skiplist.put("banana", "value3", 0);
    skiplist.put("cherry1", "value4", 0);
    skiplist.put("cherry2", "value4", 0);

    SkipListIterator it = SkipListIterator();

    // 测试前缀的起始位置
    it = skiplist.begin_preffix("app");
    EXPECT_EQ(it.get_key(), "apple1");

    it = skiplist.begin_preffix("ban");
    EXPECT_EQ(it.get_key(), "banana");

    it = skiplist.begin_preffix("veg");
    EXPECT_TRUE(it == skiplist.end());

    // 测试前缀的结束位置
    it = skiplist.end_preffix("app");
    EXPECT_EQ(it.get_key(), "banana");

    it = skiplist.end_preffix("che");
    EXPECT_TRUE(it == skiplist.end());
}

TEST(SkipListTest, IteratorPredicate) {
    SkipList skiplist;

    skiplist.put("preffix1", "value1", 0);
    skiplist.put("preffix2", "value2", 0);
    skiplist.put("preffix3", "value3", 0);
    skiplist.put("oversizekey", "oversizevalue", 0);
    skiplist.put("mypreffix1", "myvalue1", 0);
    skiplist.put("mypreffix2", "myvalue2", 0);
    skiplist.put("mypreffix3", "myvalue3", 0);
    skiplist.put("mypreffix4", "myvalue4", 0);

    auto preffix_result = skiplist.iters_monotony_predicate([](const std::string &key) {
        auto match_str = key.substr(0, 3);
        if (match_str == "pre") return 0;
        return (match_str < "pre") ? 1 : -1;
    });

    ASSERT_TRUE(preffix_result.has_value());
    auto [beg_it, end_it] = preffix_result.value();

    EXPECT_EQ(beg_it.get_val(), "value1");
    ++beg_it;
    EXPECT_EQ(beg_it.get_val(), "value2");
    ++beg_it;
    EXPECT_EQ(beg_it.get_val(), "value3");
    ++beg_it;
    EXPECT_TRUE(beg_it == end_it);
}

TEST(SkipListTest, TransactionTest) {
    SkipList skiplist;

    skiplist.put("key", "value1", 1);
    skiplist.put("key", "value2", 2);
    skiplist.put("key", "value3", 3);

    EXPECT_EQ(skiplist.get("key", 0).get_val(), "value3");
    EXPECT_EQ(skiplist.get("key", 1).get_val(), "value1");
    EXPECT_EQ(skiplist.get("key", 2).get_val(), "value2");
    EXPECT_EQ(skiplist.get("key", 3).get_val(), "value3");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
