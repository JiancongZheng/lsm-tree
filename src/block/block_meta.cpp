#include <stdexcept>

#include "./block_meta.h"

namespace LSMT {
BlockMeta::BlockMeta() : offset(0), fkey(""), lkey("") { }

BlockMeta::BlockMeta(size_t offset, const std::string &first_key, const std::string &last_key)
    : offset(offset), fkey(first_key), lkey(last_key) { }

void BlockMeta::encode_meta(const std::vector<BlockMeta> &meta_entries, std::vector<uint8_t> &meta_data) {
    size_t total_size = sizeof(uint32_t) + sizeof(uint32_t);  // entry number + hash value
    for (const auto &entry : meta_entries) {
        total_size += sizeof(uint32_t) + sizeof(uint16_t) + entry.fkey.size() + sizeof(uint16_t) + entry.lkey.size();
    }
    meta_data.resize(total_size);
    uint8_t* pointer = meta_data.data();

    // 写入Meta Entry数量
    uint32_t entry_number = meta_entries.size();
    memcpy(pointer, &entry_number, sizeof(uint32_t));
    pointer += sizeof(uint32_t);

    // 写入Meta Entry数据
    for (const auto &entry : meta_entries) {
        uint32_t offset32 = static_cast<uint32_t>(entry.offset);
        memcpy(pointer, &offset32, sizeof(uint32_t));
        pointer += sizeof(uint32_t);

        uint16_t fkey_len = entry.fkey.size();
        memcpy(pointer, &fkey_len, sizeof(uint16_t));
        memcpy(pointer + sizeof(uint16_t), entry.fkey.data(), fkey_len);
        pointer += sizeof(uint16_t) + fkey_len;

        uint16_t lkey_len = entry.lkey.size();
        memcpy(pointer, &lkey_len, sizeof(uint16_t));
        memcpy(pointer + sizeof(uint16_t), entry.lkey.data(), lkey_len);
        pointer += sizeof(uint16_t) + lkey_len;
    }

    // 写入Meta Entry哈希值
    size_t meta_size = total_size - sizeof(uint32_t) - sizeof(uint32_t);
    uint32_t hash_value = std::hash<std::string_view>{}(
        std::string_view(reinterpret_cast<const char*>(pointer - meta_size), meta_size)
    );
    memcpy(pointer, &hash_value, sizeof(uint32_t));
}

void BlockMeta::decode_meta(const std::vector<uint8_t> &meta_data, std::vector<BlockMeta> &meta_entries) {
    if (meta_data.size() < sizeof(uint32_t) + sizeof(uint32_t)) {
        throw std::runtime_error("Invalid Metadata Size");
    }
    const uint8_t* pointer = meta_data.data();
    
    // 读取Meta Entry数量
    uint32_t entry_number;
    memcpy(&entry_number, pointer, sizeof(uint32_t));
    pointer += sizeof(uint32_t);

    // 读取Meta Entry数据
    for (uint32_t i = 0; i < entry_number; ++i) {
        BlockMeta entry;

        uint32_t offset32;
        memcpy(&offset32, pointer, sizeof(uint32_t));
        entry.offset = offset32;
        pointer += sizeof(uint32_t);

        uint16_t fkey_len;
        memcpy(&fkey_len, pointer, sizeof(uint16_t));
        entry.fkey.assign(reinterpret_cast<const char *>(pointer + sizeof(uint16_t)), fkey_len);
        pointer += sizeof(uint16_t) + fkey_len;

        uint16_t lkey_len;
        memcpy(&lkey_len, pointer, sizeof(uint16_t));
        entry.lkey.assign(reinterpret_cast<const char *>(pointer + sizeof(uint16_t)), lkey_len);
        pointer += sizeof(uint16_t) + lkey_len;

        meta_entries.push_back(entry);
    }

    // 验证Meta Entry哈希值
    uint32_t old_hash, new_hash;
    memcpy(&old_hash, pointer, sizeof(uint32_t));

    size_t meta_size = meta_data.size() - sizeof(uint32_t) - sizeof(uint32_t);
    new_hash = std::hash<std::string_view>{}(
        std::string_view(reinterpret_cast<const char *>(pointer - meta_size), meta_size)
    );
    if (new_hash != old_hash) {
        throw std::runtime_error("Meta Data Hash Value Error");
    }
}
}  // LOG STRUCT MERGE TREE