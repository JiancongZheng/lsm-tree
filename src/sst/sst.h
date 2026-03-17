#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <stdexcept>

#include "block/block.h"
#include "block/block_cache.h"
#include "block/block_meta.h"
#include "utils/bloom_filter.h"
#include "utils/files.h"

namespace LSMT {
/**
 * --------------------------------------------------------------------------------------------------------------------------------------------
 * |           Block Section           |          Meta Section          | Bloom Filter |                   Extra Information                  |
 * --------------------------------------------------------------------------------------------------------------------------------------------
 * | Block 1 | Block 2 | ... | Block N | Number | Meta 1 | ... | Meta N |              | Meta Offset | Bloom Offset | Min TRX_ID | MAX TRX_ID |
 * --------------------------------------------------------------------------------------------------------------------------------------------
 **/
class SSTBuilder;
class SSTIterator;

class SST : public std::enable_shared_from_this<SST> {
    friend class SSTBuilder;

public:
    static std::shared_ptr<SST> open(size_t sst_id, FileObj file_obj, std::shared_ptr<BlockCache> block_cache);

    void remove();

    int64_t get_block_id(const std::string &key);
    
    std::shared_ptr<Block> get_block(size_t block_id);

    SSTIterator get(const std::string &key, uint64_t trx_id);

    std::string get_fkey() const;

    std::string get_lkey() const;

    size_t get_sst_id() const;

    size_t get_sst_size() const;

    size_t get_block_number() const;

    SSTIterator begin(uint64_t trx_id);
    
    SSTIterator end();

    std::optional<std::pair<SSTIterator, SSTIterator>> 
    iters_monotony_predicate(std::function<int(const std::string &)> predicate);

    std::pair<uint64_t, uint64_t> get_trx_id_range() const;

private:
    size_t sst_id;
    FileObj file_obj;
    std::vector<BlockMeta> meta_entries;
    uint32_t meta_section_offset;
    uint32_t bloom_filter_offset;
    std::string fkey;
    std::string lkey;
    std::shared_ptr<BloomFilter> filter;
    std::shared_ptr<BlockCache> block_cache;
    uint64_t min_trx_id;
    uint64_t max_trx_id;

    // uint8_t storage_mode = 0;  // 0=inline 1=Wisckey
    // std::shared_ptr<VLog> vlog;
};

} // LOG STRUCTURED MERGE TREE