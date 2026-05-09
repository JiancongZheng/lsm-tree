#pragma once

#include <cstdint>
#include <cstddef>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "lsm_iterator.h"

#include "config/config.h"
#include "iterator/iterator.h"
#include "memtable/memtable.h"
#include "sst/sst.h"
#include "sst/sst_builder.h"
#include "sst/sst_iterator.h"

namespace LSMT {
class LevelIterator;

class LSMTEngine : public std::enable_shared_from_this<LSMTEngine> {
public:
    LSMTEngine(std::string path);

    ~LSMTEngine() = default;

    std::optional<std::pair<std::string, uint64_t>> get(const std::string &key, uint64_t trx_id);

    std::vector<std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
    get(const std::vector<std::string> &keys, uint64_t trx_id);

    std::optional<std::pair<std::string, uint64_t>> get_unlock(const std::string &key, uint64_t trx_id);

    uint64_t put(const std::string &key, const std::string &val, uint64_t trx_id);

    uint64_t put(const std::vector<std::pair<std::string, std::string>> &kv_pairs, uint64_t trx_id);

    uint64_t remove(const std::string &key, uint64_t trx_id);

    uint64_t remove(const std::vector<std::string> &keys, uint64_t trx_id);

    void clear();

    uint64_t flush();

    std::string get_sst_path(size_t sst_index, size_t sst_level);
    
    LevelIterator begin(uint64_t trx_id);
    
    LevelIterator end();

    std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
    iter_monotony_predicate(uint64_t trx_id, std::function<int(const std::string&)> predicate);

    static size_t get_sst_size(size_t level);

private:
    void compact(size_t src_level, size_t dst_level);

    std::vector<std::shared_ptr<SST>> full_compact(std::vector<size_t> &src_indexes, 
        std::vector<size_t> &dst_indexes, size_t dst_level);
    
    std::vector<std::shared_ptr<SST>> zone_compact(std::vector<size_t> &src_indexes,
        std::vector<size_t> &dst_indexes, size_t dst_level);

    std::vector<std::shared_ptr<SST>> generate_ssts(BaseIterator &iter, size_t size, size_t level);
public:
    std::string lsmt_path;
    MemTable memtable;
    std::map<size_t, std::deque<size_t>> sst_indexes;
    std::unordered_map<size_t, std::shared_ptr<SST>> ssts;
    std::shared_mutex lsmt_mutex;
    std::shared_ptr<BlockCache> block_cache;
    size_t next_sst_index = 0;
    size_t curr_max_level = 0;
};

class LSMTree {
public:
    LSMTree(std::string lsmt_path);

    ~LSMTree();

    std::optional<std::string> get(const std::string &key);

    std::vector<std::pair<std::string, std::optional<std::string>>> get(const std::vector<std::string> &keys);

    void put(const std::string &key, const std::string &val);

    void put(const std::vector<std::pair<std::string, std::string>> &kv_pairs);

    void remove(const std::string &key);

    void remove(const std::vector<std::string> &keys);

    LevelIterator begin(uint64_t trx_id);

    LevelIterator end();

    std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
    iter_monotony_predicate(uint64_t trx_id, std::function<int(const std::string&)> predicate);

    void clear();

    void flush_one();

    void flush_all();

private:
    std::shared_ptr<LSMTEngine> engine;
};
} // LOG STRUCTURED MERGE TREE