#include "sst.h"
#include "sst_builder.h"
#include "config/config.h"

namespace LSMT {

SSTBuilder::SSTBuilder(size_t block_size, bool has_bloom) : block(block_size) {
    if (has_bloom) {
        bloom_filter = std::make_shared<BloomFilter>(
            TomlConfig::get_instance().get_bloom_filter_expected_elements(),
            TomlConfig::get_instance().get_bloom_filter_false_positive_rate()
        );
    }
    block_size = block_size;
    min_trx_id = UINT64_MAX;
    max_trx_id = 0;
}

void SSTBuilder::add(const std::string &key, const std::string &val, uint64_t trx_id) {
    if (bloom_filter != nullptr) {
        bloom_filter->add(key);
    }
    min_trx_id = std::min(min_trx_id, trx_id);
    max_trx_id = std::max(max_trx_id, trx_id);

    bool force_write = (key == lkey);

    if (block.add_entry(key, val, trx_id, force_write) == true) {
        fkey = fkey.empty() ? key : fkey;
        lkey = key;
    } else {
        finish_block();
        block.add_entry(key, val, trx_id, false);
        fkey = key;
        lkey = key;
    }
}

size_t SSTBuilder::estimated_size() const {
    return data.size();
}

size_t SSTBuilder::real_size() const {
    return data.size() + block.get_cur_size();
}

void SSTBuilder::finish_block() {
    auto old_block = std::move(this->block);
    auto encoded_data = old_block.encode();

    meta_entries.emplace_back(data.size(), fkey, lkey);

    data.insert(data.end(), encoded_data.begin(), encoded_data.end());
}

std::shared_ptr<SST> SSTBuilder::build(size_t sst_id, const std::string &path, std::shared_ptr<BlockCache> block_cache) {
    if (block.is_empty() == false) {
        finish_block();
    }

    if (meta_entries.empty()) {
        throw std::runtime_error("Cannot Build an Empty SST");
    }

    // 获取Meta Section编码和偏移量
    std::vector<uint8_t> meta_section_data;
    BlockMeta::encode_meta(meta_entries, meta_section_data);
    uint32_t meta_section_offset = data.size();

    // 获取Bloom Filter编码和偏移量
    std::vector<uint8_t> bloom_filter_data;
    if (bloom_filter != nullptr) {
        bloom_filter_data = bloom_filter->encode();
    }
    uint32_t bloom_filter_offset = data.size() + meta_section_data.size();
    
    // 依次写入DataSection MetaSection BloomFilter ExtraInformation
    size_t write_offset = 0;
    FileObj file_obj = FileObj::create_and_write(path, {});
    if (!data.empty() && !file_obj.write(write_offset, data)) {
        throw std::runtime_error("Failed To Write Block Section in " + path);
    }
    write_offset += data.size();

    if (!meta_section_data.empty() && !file_obj.write(write_offset, meta_section_data)) {
        throw std::runtime_error("Failed To Write Meta Section in " + path);
    }
    write_offset += meta_section_data.size();

    if (!bloom_filter_data.empty() && !file_obj.write(write_offset, bloom_filter_data)) {
        throw std::runtime_error("Failed To Write Bloom Filter in " + path);
    }
    write_offset += bloom_filter_data.size();

    if (!file_obj.write_uint32(write_offset, meta_section_offset)) {
        throw std::runtime_error("Failed To Write Meta Offset in " + path);
    }
    write_offset += sizeof(uint32_t);

    if (!file_obj.write_uint32(write_offset, bloom_filter_offset)) {
        throw std::runtime_error("Failed To Write Bloom Offset in " + path);
    }
    write_offset += sizeof(uint32_t);

    if (!file_obj.write_uint64(write_offset, min_trx_id)) {
        throw std::runtime_error("Failed To Write Min Transaction Id in " + path);
    }
    write_offset += sizeof(uint64_t);

    if (!file_obj.write_uint64(write_offset, max_trx_id)) {
        throw std::runtime_error("Failed To Write Max Transaction Id in " + path);
    }
    write_offset += sizeof(uint64_t);

    if (!file_obj.sync()) {
        throw std::runtime_error("Failed To Sync File " + path);
    }

    auto result = std::make_shared<SST>();

    result->sst_id = sst_id;
    result->file_obj = std::move(file_obj);
    result->meta_entries = meta_entries;
    result->bloom_filter_offset = bloom_filter_offset;
    result->meta_section_offset = meta_section_offset;
    result->fkey = meta_entries.front().fkey;
    result->lkey = meta_entries.back().lkey;
    result->filter = bloom_filter;
    result->block_cache = block_cache;
    result->min_trx_id = min_trx_id;
    result->max_trx_id = max_trx_id;

    return result;
}

} // LOG STRUCTURED MERGE TREE
