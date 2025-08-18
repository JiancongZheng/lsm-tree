#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "memtable_iterator.h"
#include "../skiplist/skiplist.h"

namespace LSMT {
class MemTable {
public:
    MemTable();

    ~MemTable() = default;

    void put(const std::string &key, const std::string &val, uint64_t trx_id);

    void put(const std::vector<std::pair<std::string, std::string>> &kv_pairs, uint64_t trx_id);

    void remove(const std::string &key, uint64_t trx_id);

    void remove(const std::vector<std::string> &keys, uint64_t trx_id);

    SkipListIterator get(const std::string &key, uint64_t trx_id);

    std::vector<std::pair<std::string, std::optional<std::string>>> 
    get(const std::vector<std::string> &keys, uint64_t trx_id);

    MemtableIterator begin(uint64_t trx_id);

    MemtableIterator end();
    
    MemtableIterator iters_preffix(const std::string &preffix, uint64_t trx_id);

    std::optional<std::pair<MemtableIterator, MemtableIterator>> 
    iters_monotony_predicate(uint64_t trx_id, std::function<int(const std::string&)> predicate);

    size_t get_total_size();

    size_t get_active_size();

    size_t get_frozen_size();

    void freeze_memtable();

private:
    void freeze_active_table();
    
    inline bool get_active(const std::string &key, uint64_t trx_id, SkipListIterator &it);

    inline bool get_frozen(const std::string &key, uint64_t trx_id, SkipListIterator &it);

private:
    std::shared_ptr<SkipList> active_table;
    std::vector<std::shared_ptr<SkipList>> frozen_table;
    size_t frozen_bytes;
    std::shared_mutex active_mutex;
    std::shared_mutex frozen_mutex;
};
}  // LOG STRUCT MERGE TREE