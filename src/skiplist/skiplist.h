#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "skiplist_iterator.h"

namespace LSMT {
struct SkipListNode {
    std::string key;
    std::string val;
    uint64_t trx_id;
    std::vector<std::shared_ptr<SkipListNode>> next;

    SkipListNode(const std::string &k, const std::string &v, int level, uint64_t trx_id);

    bool operator<(const SkipListNode &other) const;

    bool operator>(const SkipListNode &other) const;

    bool operator==(const SkipListNode &other) const;

    bool operator!=(const SkipListNode &other) const;
};

class SkipList {
public:
    SkipList(int max_level = 16);

    ~SkipList();

    void put(const std::string &key, const std::string &val, uint64_t trx_id);

    SkipListIterator get(const std::string &key, uint64_t trx_id);
    
    void remove(const std::string &key);

    std::vector<std::tuple<std::string, std::string, uint64_t>> flush();

    size_t get_size();

    void clear();

    SkipListIterator begin();
    
    SkipListIterator begin_preffix(const std::string &preffix);
    
    SkipListIterator end();

    SkipListIterator end_preffix(const std::string &preffix);

    std::optional<std::pair<SkipListIterator, SkipListIterator>> 
    iters_monotony_predicate(std::function<int(const std::string &)> predicate);

private:
    int random_level();

private:
    std::shared_ptr<SkipListNode> head;
    int max_level;
    int cur_level;
    size_t size_bytes = 0;
    
    std::mt19937 gen;
    std::uniform_int_distribution<> dis_binomary;
};
}  // LOG STRUCT MERGE TREE
