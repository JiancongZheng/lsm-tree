#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <thread>

#include "../src/memtable/memtable.h"

using namespace ::LSMT;

TEST(MemTableTest, BasicOperation) {
    MemTable memtable;

    memtable.put("key", "old_value", 0);
    EXPECT_EQ(memtable.get("key", 0).get_val(), "old_value");

    memtable.put("key", "new_value", 0);
    EXPECT_EQ(memtable.get("key", 0).get_val(), "new_value");

    memtable.remove("key", 0);
    EXPECT_EQ(memtable.get("key", 0).get_val().empty(), true);

    EXPECT_FALSE(memtable.get("nonekey", 0).is_valid());
}

TEST(MemTableTest, BatchOperation) {
    MemTable memtable;
    const int number = 1000;
    std::vector<std::pair<std::string, std::string>> kv_pairs;
    std::vector<std::string> keys, vals;

    for (int i = 0; i < number; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string val = "val" + std::to_string(i);
        kv_pairs.emplace_back(key, val);
        keys.emplace_back(key);
        vals.emplace_back(val);
    }

    memtable.put(kv_pairs, 0);

    auto results = memtable.get(keys, 0);

    for (int i = 0; i < number; ++i) {
        EXPECT_EQ(results[i].second.value(), vals[i]);
    }
}

TEST(MemTableTest, FrozenActiveTable) {
    MemTable memtable;

    memtable.put("key1", "value1", 0);
    memtable.put("key2", "value2", 0);

    memtable.freeze_memtable();
    memtable.put("key3", "value3", 0);
    memtable.put("key4", "value4", 0);

    memtable.freeze_memtable();
    memtable.put("key5", "value5", 0);
    memtable.put("key6", "value6", 0);

    EXPECT_EQ(memtable.get("key1", 0).get_val(), "value1");
    EXPECT_EQ(memtable.get("key2", 0).get_val(), "value2");
    EXPECT_EQ(memtable.get("key3", 0).get_val(), "value3");
    EXPECT_EQ(memtable.get("key5", 0).get_val(), "value5");
    EXPECT_EQ(memtable.get("key6", 0).get_val(), "value6");
}

TEST(MemTableTest, MemorySizeTracking) {
    MemTable memtable;

    EXPECT_EQ(memtable.get_total_size(), 0);

    memtable.put("key1", "value1", 0);
    memtable.put("key2", "value2", 0);

    EXPECT_NE(memtable.get_active_size(), 0);
    EXPECT_EQ(memtable.get_frozen_size(), 0);

    memtable.freeze_memtable();

    EXPECT_EQ(memtable.get_active_size(), 0);
    EXPECT_NE(memtable.get_frozen_size(), 0);
}

TEST(MemTableTest, IteratorOperations) {
    MemTable memtable;

    memtable.put("key1", "oldvalue1", 0);
    memtable.put("key2", "oldvalue2", 0);
    memtable.put("key3", "oldvalue3", 0);

    std::vector<std::pair<std::string, std::string>> result1;
    for (auto it = memtable.begin(0); it != memtable.end(); ++it) {
        result1.push_back(*it);
    }
    ASSERT_EQ(result1.size(), 3);
    EXPECT_EQ(result1[0].second, "oldvalue1");
    EXPECT_EQ(result1[1].second, "oldvalue2");
    EXPECT_EQ(result1[2].second, "oldvalue3");

    memtable.freeze_memtable();

    memtable.put("key2", "newvalue2", 0);
    memtable.put("key4", "oldvalue4", 0);
    memtable.remove("key1", 0);

    std::vector<std::pair<std::string, std::string>> result2;
    for (auto it = memtable.begin(0); it != memtable.end(); ++it) {
        result2.push_back(*it);
    }

    ASSERT_EQ(result2.size(), 3); 
    EXPECT_EQ(result2[0].second, "newvalue2");
    EXPECT_EQ(result2[1].second, "oldvalue3");
    EXPECT_EQ(result2[2].second, "oldvalue4");

    memtable.freeze_memtable();
}

TEST(MemTableTest, ConcurrentOperation) {
    MemTable memtable;
    const int num_readers = 4;
    const int num_writers = 2;
    const int num_operations = 1000;

    std::atomic<bool> start(false);
    std::atomic<int>  thread_counter(num_readers + num_writers);

    std::vector<std::string> insert_keys;
    std::shared_mutex keys_mutex;

    auto writer_func = [&](int thread_id) {
        while (!start) { std::this_thread::yield(); }

        for (int i = 0; i < num_operations; ++i) {
            std::string key = "key_" + std::to_string(thread_id) + "_" + std::to_string(i);
            std::string val = "val_" + std::to_string(thread_id) + "_" + std::to_string(i);
            memtable.put(key, val, 0);
            {
                std::unique_lock<std::shared_mutex> lock(keys_mutex);
                insert_keys.push_back(key);
            }
            std::this_thread::sleep_for(std::chrono::microseconds(rand() % 100));
        }

        thread_counter.fetch_sub(1);
    };

    auto reader_func = [&](int thread_id) {
        while (!start) { std::this_thread::yield(); }

        int found_count = 0;
        for (int i = 0; i < num_operations; ++i) {
            std::string tofind_key;
            {
                std::unique_lock<std::shared_mutex> lock(keys_mutex);
                if (!insert_keys.empty()) {
                    tofind_key = insert_keys[rand() % insert_keys.size()];
                }
            }
            if (!tofind_key.empty()) {
                auto result = memtable.get(tofind_key, 0);
                if (result.is_valid()) { ++found_count; }
            }
            std::this_thread::sleep_for(std::chrono::microseconds(rand() % 50));
        }

        thread_counter.fetch_sub(1);
    };

    std::vector<std::thread> writers;
    std::vector<std::thread> readers;

    for (int i = 0; i < num_writers; ++i) { writers.emplace_back(writer_func, i); }
    for (int i = 0; i < num_readers; ++i) { readers.emplace_back(reader_func, i); }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    start = true;

    while (thread_counter.load() != 0) { std::this_thread::yield(); }

    for (auto &writer : writers) { writer.join(); }
    for (auto &reader : readers) { reader.join(); }
}

TEST(MemTableTest, IteratorPreffix) {
    MemTable memtable;

    memtable.put("apple1", "apple1", 0);
    memtable.put("apply1", "apply1", 0);
    memtable.put("apply2", "apply2", 0);

    memtable.freeze_memtable();

    memtable.put("apple2", "apple2", 0);
    memtable.put("apple3", "apple3", 0);
    memtable.put("aptitude1", "aptitude1", 0);
    memtable.put("aptitude2", "aptitude2", 0);
    memtable.put("aptitude3", "aptitude3", 0);

    memtable.freeze_memtable();

    memtable.put("apical1", "apical1", 0);
    memtable.put("apical2", "apical2", 0);

    int idx = 0;
    std::vector<std::string> result = {"apple1", "apple2", "apple3", "apply1", "apply2"};

    for (auto it = memtable.iters_preffix("app", 0); !it.is_end(); ++it) {
        EXPECT_EQ(it->first, result[idx++]);
    }
}

TEST(MemTableTest, IteratorPredicate) {
    MemTable memtable;

    memtable.put("apical1", "apical1", 0);
    memtable.put("apical2", "apical2", 0);
    memtable.put("apple1", "apple1", 0);
    memtable.put("apple2", "apple2", 0);
    memtable.put("apple3", "apple3", 0);
    memtable.put("aptitude1", "aptitude1", 0);
    memtable.put("aptitude2", "aptitude2", 0);

    auto preffix_result = memtable.iters_monotony_predicate(0, [](const std::string &key) {
        auto match_str = key.substr(0, 3);
        if (match_str == "app") return 0;
        return (match_str < "app") ? 1 : -1;
    });

    ASSERT_TRUE(preffix_result.has_value());
    auto [beg_it, end_it] = preffix_result.value();

    EXPECT_EQ(beg_it->first, "apple1");
    ++beg_it;
    EXPECT_EQ(beg_it->first, "apple2");
    ++beg_it;
    EXPECT_EQ(beg_it->first, "apple3");
    ++beg_it;
    EXPECT_TRUE(beg_it == end_it);
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}