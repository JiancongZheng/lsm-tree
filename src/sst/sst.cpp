#include "sst.h"
#include "sst_iterator.h"

namespace LSMT {
std::shared_ptr<SST> SST::open(size_t sst_id, FileObj file_obj, std::shared_ptr<BlockCache> block_cache) {
    auto sst = std::make_shared<SST>();
    sst->sst_id = sst_id;
    sst->file_obj = std::move(file_obj);
    sst->block_cache = block_cache;

    size_t read_position = sst->file_obj.size();

    // 读取EXTRA INFORMATION
    read_position -= sizeof(uint64_t);
    sst->max_trx_id = sst->file_obj.read_uint64(read_position);
    read_position -= sizeof(uint64_t);
    sst->min_trx_id = sst->file_obj.read_uint64(read_position);
    read_position -= sizeof(uint32_t);
    sst->bloom_filter_offset = sst->file_obj.read_uint32(read_position);
    read_position -= sizeof(uint32_t);
    sst->meta_section_offset = sst->file_obj.read_uint32(read_position);

    // 读取Bloom Filter
    size_t bloom_filter_size = read_position - sst->bloom_filter_offset;
    if (bloom_filter_size > 0) {
        std::vector<uint8_t> data = sst->file_obj.read(sst->bloom_filter_offset, bloom_filter_size);
        sst->filter = std::make_shared<BloomFilter>(BloomFilter::decode(data));
    }

    // 读取Meta Section
    size_t meta_section_size = sst->bloom_filter_offset - sst->meta_section_offset;
    if (meta_section_size > 0) {
        std::vector<uint8_t> data = sst->file_obj.read(sst->meta_section_offset, meta_section_size);
        BlockMeta::decode_meta(data, sst->meta_entries);
    }

    // 读取首Key值和尾Key值
    if (!sst->meta_entries.empty()) {
        sst->fkey = sst->meta_entries.front().fkey;
        sst->lkey = sst->meta_entries.back().lkey;
    }

    return sst;
}

void SST::remove() {
    file_obj.remove();
}

int64_t SST::get_block_id(const std::string &key) {
    if (filter && !filter->possibly_contain(key) || key < fkey || key > lkey) {
        return -1;
    }

    int lk = 0, rk = meta_entries.size() - 1;
    while (lk <= rk) {
        int mid = lk + (rk - lk) / 2;
        if (key >= meta_entries[mid].fkey && key <= meta_entries[mid].lkey) {
            return mid;
        } else if (key < meta_entries[mid].fkey) {
            rk = mid - 1;
        } else if (key > meta_entries[mid].lkey) {
            lk = mid + 1;
        } else {
            // Nothing to do
        }
    }

    return -1;
}
    
std::shared_ptr<Block> SST::get_block(size_t block_id) {
    if (block_cache != nullptr) {
        auto block = block_cache->get(sst_id, block_id);
        if (block != nullptr) {
            return block;
        }
    } else {
        throw std::runtime_error("Block cache is not initialized");
    }

    const BlockMeta meta_entry = meta_entries[block_id];

    size_t block_size;
    if (block_id == meta_entries.size() - 1) {
        block_size = meta_section_offset - meta_entry.offset;
    } else {
        block_size = meta_entries[block_id + 1].offset - meta_entry.offset;
    }

    std::vector<uint8_t> data = file_obj.read(meta_entry.offset, block_size);
    std::shared_ptr<Block> block = Block::decode(data);

    if (block_cache != nullptr) {
        block_cache->put(sst_id, block_id, block);
    } else {
        throw std::runtime_error("Block cache is not initialized");
    }

    return block;
}

SSTIterator SST::get(const std::string &key, uint64_t trx_id) {
    if (key < fkey || key > lkey || (filter && !filter->possibly_contain(key))) {
        return this->end();
    }
    return SSTIterator(shared_from_this(), key, trx_id);
}

std::string SST::get_fkey() const{
    return fkey;
}

std::string SST::get_lkey() const {
    return lkey;
}

size_t SST::get_sst_id() const {
    return sst_id;
}

size_t SST::get_sst_size() const {
    return file_obj.size();
}

size_t SST::get_block_number() const {
    return meta_entries.size();
}

SSTIterator SST::begin(uint64_t trx_id) {
    return SSTIterator(shared_from_this(), trx_id);
}
 
SSTIterator SST::end() {
    SSTIterator iterator(shared_from_this(), 0);
    iterator.block_id = meta_entries.size();
    iterator.block_it = nullptr;
    return iterator;
}

std::optional<std::pair<SSTIterator, SSTIterator>> 
SST::iters_monotony_predicate(std::function<int(const std::string &)> predicate) {
    std::optional<SSTIterator> final_beg;
    std::optional<SSTIterator> final_end;

    for (int block_id = 0; block_id < meta_entries.size(); block_id++) {
        if (predicate(meta_entries[block_id].fkey) < 0) {
            break;
        }
        if (predicate(meta_entries[block_id].lkey) > 0) {
            continue;
        }

        auto block = get_block(block_id);
        auto result = block->get_monotony_predicate_iters(max_trx_id, predicate);
        if (result.has_value()) {
            auto [beg, end] = result.value();
            if (final_beg.has_value() == false) {
                final_beg = SSTIterator(shared_from_this(), max_trx_id);
                final_beg->block_id = block_id;
                final_beg->block_it = beg;                
            }
            final_end = SSTIterator(shared_from_this(), max_trx_id);
            final_end->block_id = block_id;
            final_end->block_it = end;
            if (final_end->is_end() && final_end->block_id == get_block_number()) {
                final_end = std::nullopt;
            }
        }        
    }

    if (!final_beg.has_value() || !final_end.has_value()) {
        return std::nullopt;
    }
    return std::make_pair(final_beg.value(), final_end.value());
}

std::pair<uint64_t, uint64_t> SST::get_trx_id_range() const {
    return std::make_pair(min_trx_id, max_trx_id);
}
} // LOG STRUCTURED MERGE TREE