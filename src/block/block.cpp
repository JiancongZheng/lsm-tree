#include "block.h"
#include "block_iterator.h"

namespace LSMT {
Block::Block(size_t capacity) : capacity(capacity) { }

std::vector<uint8_t> Block::encode(bool with_hash) {
    size_t total_bytes = data.size() * sizeof(uint8_t) + offsets.size() * sizeof(uint16_t) + sizeof(uint16_t);
    if (with_hash == true) {
        total_bytes += sizeof(uint32_t);
    }
    std::vector<uint8_t> encoded(total_bytes, 0);

    // 复制元素数据段
    memcpy(encoded.data(), data.data(), data.size() * sizeof(uint8_t));

    // 复制元素偏移段
    size_t offset_pos = data.size() * sizeof(uint8_t);
    memcpy(encoded.data() + offset_pos, offsets.data(), offsets.size() * sizeof(uint16_t));

    // 复制元素数量和哈希值
    size_t number_pos = data.size() * sizeof(uint8_t) + offsets.size() * sizeof(uint16_t);
    uint16_t entry_num = offsets.size();
    memcpy(encoded.data() + number_pos, &entry_num, sizeof(uint16_t));
    if (with_hash == true) {
        uint32_t hash_value = std::hash<std::string_view>{}(
            std::string_view(reinterpret_cast<const char*>(encoded.data()), encoded.size() - sizeof(uint32_t))
        );
        memcpy(encoded.data() + number_pos + sizeof(uint16_t), &hash_value, sizeof(uint32_t));
    }

    return encoded;
}

std::shared_ptr<Block> Block::decode(const std::vector<uint8_t> &encoded, bool with_hash) {
    if (encoded.size() <= sizeof(uint16_t) || with_hash && encoded.size() <= sizeof(uint16_t) + sizeof(uint32_t)) {
        throw std::runtime_error("Encoded Data Too Small");
    }

    auto block = std::make_shared<Block>();

    // 安全检查和复制元素数量
    uint16_t entry_num;
    size_t number_pos = encoded.size() - sizeof(uint16_t);
    if (with_hash == true) {
        number_pos = number_pos - sizeof(uint32_t);
        uint32_t old_hash_value;
        memcpy(&old_hash_value, encoded.data() + encoded.size() - sizeof(uint32_t), sizeof(uint32_t));
        uint32_t new_hash_value = std::hash<std::string_view>{}(
            std::string_view(reinterpret_cast<const char*>(encoded.data()), encoded.size() - sizeof(uint32_t))
        );
        if (old_hash_value != new_hash_value) {
            throw std::runtime_error("Block Hash Verification Error");
        }
    }
    memcpy(&entry_num, encoded.data() + number_pos, sizeof(uint16_t));

    //TODO 对大端序小端序场景的适配工作
    // 复制元素偏移段
    size_t offset_pos = number_pos - entry_num * sizeof(uint16_t);
    block->offsets.resize(entry_num);
    memcpy(block->offsets.data(), encoded.data() + offset_pos, entry_num * sizeof(uint16_t));

    // 复制元素数据段
    block->data.reserve(offset_pos);
    block->data.assign(encoded.begin(), encoded.begin() + offset_pos);

    return block;
}

bool  Block::add_entry(const std::string &key, const std::string &val, uint64_t trx_id, bool force_write) {
    size_t total_size = key.size() + sizeof(uint16_t) + 
                        val.size() + sizeof(uint16_t) + 
                        sizeof(uint64_t) + sizeof(uint16_t) + get_cur_size();
    if (!force_write && total_size > capacity) {
        return false;
    }
    // 计算Entry大小并分配空间
    size_t entry_size = sizeof(uint16_t) + key.size() + sizeof(uint16_t) + val.size() + sizeof(uint64_t);
    size_t write_size = data.size();
    data.resize(write_size + entry_size);

    // 写入key_len和key数据
    uint16_t key_len = key.size();
    memcpy(data.data() + write_size, &key_len, sizeof(uint16_t));
    memcpy(data.data() + write_size + sizeof(uint16_t), key.data(), key_len);

    write_size += sizeof(uint16_t) + key_len;

    // 写入val_len和val数据
    uint16_t val_len = val.size();
    memcpy(data.data() + write_size, &val_len, sizeof(uint16_t));
    memcpy(data.data() + write_size + sizeof(uint16_t), val.data(), val_len);

    write_size += sizeof(uint16_t) + val_len;

    // 写入transaction id数据
    memcpy(data.data() + write_size, &trx_id, sizeof(uint64_t));

    write_size += sizeof(uint64_t);

    // 写入偏移段数据
    offsets.push_back(write_size - entry_size);
    
    return true;
}

std::string Block::get_first_key() {
    if (data.empty() || offsets.empty()) {
        return "";
    }
    uint16_t key_len;
    memcpy(&key_len, data.data(), sizeof(uint16_t));
    
    std::string key(reinterpret_cast<char*>(data.data() + sizeof(uint16_t)), key_len);

    return key;
}

size_t Block::get_offset(size_t index) const {
    if (index > offsets.size()) {
        throw std::runtime_error("Index Out Of Offsets Range");
    }
    return offsets[index];
}


Entry Block::get_entry_by_offset(size_t offset) const {
    Entry entry;
    entry.key = get_key_by_offset(offset);
    entry.val = get_val_by_offset(offset);
    entry.trx_id = get_trx_id_by_offset(offset);
    return entry;
}

std::string Block::get_key_by_offset(size_t offset) const {
    uint16_t key_len;
    memcpy(&key_len, data.data() + offset, sizeof(uint16_t));
    return std::string(reinterpret_cast<const char*>(data.data() + offset + sizeof(uint16_t)), key_len);
}

std::string Block::get_val_by_offset(size_t offset) const {
    uint16_t key_len, val_len;
    memcpy(&key_len, data.data() + offset, sizeof(uint16_t));
    offset += sizeof(uint16_t) + key_len;
    memcpy(&val_len, data.data() + offset, sizeof(uint16_t));
    return std::string(reinterpret_cast<const char*>(data.data() + offset + sizeof(uint16_t)), val_len);
}

uint64_t Block::get_trx_id_by_offset(size_t offset) const {
    uint16_t key_len, val_len;
    memcpy(&key_len, data.data() + offset, sizeof(uint16_t));
    offset += sizeof(uint16_t) + key_len;
    memcpy(&val_len, data.data() + offset, sizeof(uint16_t));
    offset += sizeof(uint16_t) + val_len;
    uint64_t transaction_id;
    memcpy(&transaction_id, data.data() + offset, sizeof(uint64_t));
    return transaction_id;
}

int Block::compare_key_by_offset(size_t offset, const std::string &target_key) const { //!
    std::string source_key = get_key_by_offset(offset);
    return source_key.compare(target_key);
}

bool  Block::is_same_key(size_t index, const std::string &compare_key) const {
    if (index > offsets.size()) {
        return false;
    }
    return get_key_by_offset(offsets[index]) == compare_key;
}

int Block::adjust_idx_by_trx_id(size_t index, uint64_t trx_id) {
    size_t offset = get_offset(index);
    std::string key = get_key_by_offset(offset);
    
    if (trx_id == 0) {
        while (index > 0 && is_same_key(index - 1, key)) {
            index--;
        }
        return index;
    } else {
        uint64_t curr_trx_id = get_trx_id_by_offset(offset);
        if (curr_trx_id <= trx_id) {
            size_t prev_idx = index;
            while (prev_idx > 0 && is_same_key(prev_idx - 1, key)) {
                prev_idx--;
                if (get_trx_id_by_offset(offsets[prev_idx]) > trx_id) {
                    return prev_idx + 1;
                }
            }
            return prev_idx;
        } else {
            size_t next_idx = index + 1;
            while (next_idx < offsets.size() && is_same_key(next_idx, key)) {
                if (get_trx_id_by_offset(offsets[next_idx]) <= trx_id) {
                    return next_idx;
                }
                next_idx++;
            }
            return -1;
        }
    }
}

//TODO 调整是使用index还是offset更合适
std::optional<std::string> Block::get_val_binary(const std::string &key, uint64_t trx_id) {
    auto index = get_idx_binary(key, trx_id);
    if (!index.has_value()) {
        return std::nullopt;
    }
    return get_val_by_offset(offsets[index.value()]);
}

std::optional<size_t> Block::get_idx_binary(const std::string &key, uint64_t trx_id) {
    if (offsets.empty()) {
        return std::nullopt;
    }
    int lk = 0, rk = offsets.size() - 1;
    while (lk <= rk) {
        int mid = lk + (rk - lk) / 2;
        size_t offset = offsets[mid];
        int cmp = compare_key_by_offset(offset, key);
        if (cmp == 0) {
            auto index = adjust_idx_by_trx_id(mid, trx_id);
            return index == -1 ? std::nullopt : std::optional<int>(index);
        } else if (cmp < 0) {
            lk = mid + 1;
        } else if (cmp > 0) {
            rk = mid - 1;
        } else {
            // nothing to do
        }
    }
    return std::nullopt;
}

size_t Block::get_max_size() const {
    return capacity;
}

size_t Block::get_cur_size() const {
    return data.size() * sizeof(uint8_t) + offsets.size() * sizeof(uint16_t) + sizeof(uint16_t);
}

bool Block::is_empty() const {
    return offsets.size() == 0;
}

BlockIterator Block::begin(uint64_t trx_id) {
    return BlockIterator(shared_from_this(), 0, trx_id);
}

BlockIterator Block::end() {
    return BlockIterator(shared_from_this(), offsets.size(), 0);
}

//! 使用std::optional<std::pair<BlockIterator, BlockIterator>> + std::move替换是否可行
std::optional<std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::get_monotony_predicate_iters(uint64_t trx_id, std::function<int(const std::string&)> predicate) {
    // 获取满足predicate谓词条件的起始索引
    int lk = 0, rk = offsets.size() - 1;
    while (lk <= rk) {
        int mid = lk + (rk - lk) / 2;
        if (predicate(get_key_by_offset(offsets[mid])) <= 0) {
            rk = mid - 1;
        } else {
            lk = mid + 1;
        }
    }
    if (lk >= offsets.size() || predicate(get_key_by_offset(offsets[lk])) != 0) {
        return std::nullopt;
    }
    auto lptr = std::make_shared<BlockIterator>(shared_from_this(), lk, trx_id);

    // 获取满足predicate谓词条件的末尾索引
    lk = 0, rk = offsets.size() - 1;
    while (lk <= rk) {
        int mid = lk + (rk - lk) / 2;
        if (predicate(get_key_by_offset(offsets[mid])) >= 0) {
            lk = mid + 1;
        } else {
            rk = mid - 1;
        }
    }
    auto rptr = std::make_shared<BlockIterator>(shared_from_this(), lk, trx_id);

    // return std::make_optional(std::pair{lptr, rptr});  // 需要C++17支持模板参数推导
    return std::make_optional<std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>(lptr, rptr);
}

std::optional<std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
Block::iters_preffix(uint64_t trx_id, const std::string &preffix) {
    auto predicate = [&preffix](const std::string &key) {
        return -key.compare(0, preffix.size(), preffix);
    };
    return get_monotony_predicate_iters(trx_id, predicate);
}
} // LOG STRUCTURED MERGE TREE