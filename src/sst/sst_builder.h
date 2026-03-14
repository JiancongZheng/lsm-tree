#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "../block/block.h"
#include "../block/block_cache.h"
#include "../block/block_meta.h"
#include "../utils/bloom_filter.h"
#include "../utils/files.h"

namespace LSMT {
class SST;

class SSTBuilder {
public:
    SSTBuilder(size_t block_size, bool has_bloom);

    void add(const std::string &key, const std::string &val, uint64_t trx_id);

    size_t estimated_size() const;

    size_t real_size() const;

    void finish_block();

    std::shared_ptr<SST> build(size_t sst_id, const std::string &path, std::shared_ptr<BlockCache> block_cache);

private:
    Block block;
    std::string fkey;
    std::string lkey;
    std::vector<BlockMeta> meta_entries;
    std::vector<uint8_t> data;
    std::shared_ptr<BloomFilter> bloom_filter;
    size_t block_size;
    uint64_t min_trx_id;
    uint64_t max_trx_id;
};

} // LOG STRUCTURED MERGE TREE