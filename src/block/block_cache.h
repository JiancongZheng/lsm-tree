#pragma once

#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "./block.h"

namespace LSMT {
struct CacheItem {
    int sst_id;  //TODO sst_id为什么是int类型
    int blk_id;  //TODO blk_id为什么是int类型
    uint64_t access_count;
    std::shared_ptr<Block> block;
    CacheItem(int sst_id, int blk_id, uint64_t count, std::shared_ptr<Block> blk) 
    : sst_id(sst_id), blk_id(blk_id), access_count(count), block(blk) { }
};

// 自定义的哈希函数和等价函数 用于自定义unordered_map类功能
struct PairHash {
    template<class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2> &pair) const {
        return std::hash<T1>{}(pair.first) ^ std::hash<T2>{}(pair.second);
    }
};

struct PairEqual {
    template<class T1, class T2>
    bool operator()(const std::pair<T1, T2> &p1, const std::pair<T1, T2> &p2) const {
        return p1.first == p2.first && p1.second == p2.second;
    }
};

class BlockCache {
public:
    BlockCache(size_t capacity, size_t k);

    ~BlockCache();

    std::shared_ptr<Block> get(int sst_id, int block_id);

    void put(int sst_id, int block_id, std::shared_ptr<Block> block);

    double hit_rate() const;

private:
    void update_access_count(std::list<CacheItem>::iterator it);
private:
    size_t capacity;
    size_t K;
    mutable std::mutex cache_mutex;
    std::list<CacheItem> lru_cache_more_k;
    std::list<CacheItem> lru_cache_less_k;
    std::unordered_map<std::pair<int, int>, std::list<CacheItem>::iterator, PairHash, PairEqual> hashmap;
    mutable size_t hit_requests;
    mutable size_t sum_requests;
};
} // LOG STRUCTURED MERGE TREE