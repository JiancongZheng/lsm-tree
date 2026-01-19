#include "./block_cache.h"

namespace LSMT {
BlockCache::BlockCache(size_t capacity, size_t k) : capacity(capacity), K(k) {
    hit_requests = 0;
    sum_requests = 0;
}

BlockCache::~BlockCache() = default;

std::shared_ptr<Block> BlockCache::get(int sst_id, int block_id) {
    std::lock_guard<std::mutex> lock(cache_mutex);

    auto key = std::make_pair(sst_id, block_id);

    sum_requests++;
    if (hashmap.find(key) == hashmap.end()) {
        return nullptr;
    }
    hit_requests++;
    update_access_count(hashmap[key]);
    return hashmap[key]->block;
}

void BlockCache::put(int sst_id, int block_id, std::shared_ptr<Block> block) {
    std::lock_guard<std::mutex> lock(cache_mutex);

    auto key = std::make_pair(sst_id, block_id);

    if (hashmap.find(key) != hashmap.end()) { return; }

    if (hashmap.size() >= capacity) {
        if (!lru_cache_less_k.empty()) {
            int last_sst_id = lru_cache_less_k.back().sst_id;
            int last_blk_id = lru_cache_less_k.back().blk_id;
            hashmap.erase(std::make_pair(last_sst_id, last_blk_id));
            lru_cache_less_k.pop_back();
        } else {
            int last_sst_id = lru_cache_more_k.back().sst_id;
            int last_blk_id = lru_cache_more_k.back().blk_id;
            hashmap.erase(std::make_pair(last_sst_id, last_blk_id));
            lru_cache_more_k.pop_back();
        }
    }
    CacheItem item{sst_id, block_id, 1, block};
    lru_cache_less_k.push_front(item);
    hashmap[key] = lru_cache_less_k.begin();
}

double BlockCache::hit_rate() const {
    std::lock_guard<std::mutex> lock(cache_mutex);
    return sum_requests == 0 ? 0.0 : static_cast<double>(hit_requests) / sum_requests;
}

void BlockCache::update_access_count(std::list<CacheItem>::iterator it) {
    it->access_count++;

    if (it->access_count == K) {
        lru_cache_more_k.splice(lru_cache_more_k.begin(), lru_cache_less_k, it);
        hashmap[std::make_pair(it->sst_id, it->blk_id)] = lru_cache_more_k.begin();
    } else if (it->access_count < K) {
        lru_cache_less_k.splice(lru_cache_less_k.begin(), lru_cache_less_k, it);
    } else if (it->access_count > K) {
        lru_cache_more_k.splice(lru_cache_more_k.begin(), lru_cache_more_k, it);
    } else {
        // nothing to do
    }
}
} // LOG STRUCTURED MERGE TREE