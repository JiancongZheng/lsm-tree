#include "sst.h"
#include "sst_builder.h"

namespace LSMT {

SSTBuilder::SSTBuilder(size_t block_size, bool has_bloom) : block_size(block_size) {
    if (has_bloom) {
        bloom_filter = std::make_shared<BloomFilter>(1000, 0.1); // TODO 利用TomlConfig配置
    }
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

    std::vector<uint8_t> file_content(data.size() + meta_section_data.size() + bloom_filter_data.size() 
        + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t));
    auto write_position = file_content.data();

    // 文件内容编码 Block Section
    memcpy(write_position, data.data(), data.size() * sizeof(uint8_t));
    write_position += data.size() + sizeof(uint8_t);

    // 文件内容编码 Meta Section
    memcpy(write_position, meta_section_data.data(), meta_section_data.size() * sizeof(uint8_t));
    write_position += meta_section_data.size() * sizeof(uint8_t);

    // 文件内容编码 Bloom Filter
    memcpy(write_position, bloom_filter_data.data(), bloom_filter_data.size() * sizeof(uint8_t));
    write_position += bloom_filter_data.size() * sizeof(uint8_t);

    // 文件内容编码 Extra Information
    memcpy(write_position, &meta_section_offset, sizeof(uint32_t));
    write_position += sizeof(uint32_t);
    memcpy(write_position, &bloom_filter_offset, sizeof(uint32_t));
    write_position += sizeof(uint32_t);
    memcpy(write_position, &min_trx_id, sizeof(uint64_t));
    write_position += sizeof(uint64_t);
    memcpy(write_position, &max_trx_id, sizeof(uint64_t));

    // 使用file_content.insert()方式
    // file_content.insert(file_content.end(), reinterpret_cast<const uint8_t *>(&min_trx_id), reinterpret_cast<const uint8_t *>(&min_trx_id) + sizeof(uint64_t));
    // file_content.insert(file_content.end(), reinterpret_cast<const uint8_t *>(&max_trx_id), reinterpret_cast<const uint8_t *>(&max_trx_id) + sizeof(uint64_t));

    FileObj file_obj = FileObj::create_and_write(path, file_content);

    auto result = std::make_shared<SST>();

    result->sst_id = sst_id;
    result->file_obj = std::move(file_obj);
    result->meta_entries = std::move(meta_entries);
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