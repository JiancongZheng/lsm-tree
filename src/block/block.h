#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <vector>
#include <string>
#include <stdexcept>

#include "block_iterator.h"

/*** 
--------------------------------------------------------------------------------------
|           Data Section            |            offset Section            |  Extra  |
--------------------------------------------------------------------------------------
| Entry 1 | Entry 2 | ... | Entry N | Offset 1 | Offset 2 | ... | Offset N | Numbers |
--------------------------------------------------------------------------------------

------------------------------------------------------------------------
|                               Entry N                                |
------------------------------------------------------------------------
| key_len(2B) | key(key_len) | val_len(2B) | val(val_len) | trx_id(8B) |
------------------------------------------------------------------------
***/

namespace LSMT {
struct Entry {
    std::string key;
    std::string val;
    uint64_t trx_id;
};

class Block : public std::enable_shared_from_this<Block> {
public:
    Block() = default;

    Block(size_t capacity);

    std::vector<uint8_t> encode(bool with_hash = true);

    static std::shared_ptr<Block> decode(const std::vector<uint8_t> &encoded, bool with_hash = true);

    bool add_entry(const std::string &key, const std::string &val, uint64_t trx_id, bool force_write);

    std::string get_first_key();

    size_t get_offset(size_t index) const;

    std::optional<std::string> get_val_binary(const std::string &key, uint64_t trx_id);

    std::optional<size_t> get_idx_binary(const std::string &key, uint64_t trx_id); 

    size_t get_max_size() const;

    size_t get_cur_size() const;

    bool is_empty() const;
    
    BlockIterator begin(uint64_t trx_id = 0);

    BlockIterator end();

    std::optional<std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
    get_monotony_predicate_iters(uint64_t trx_id, std::function<int(const std::string&)> predicate);

    std::optional<std::pair<std::shared_ptr<BlockIterator>, std::shared_ptr<BlockIterator>>>
    iters_preffix(uint64_t trx_id, const std::string &preffix);

private:
    Entry get_entry_by_offset(size_t offset) const;

    std::string get_key_by_offset(size_t offset) const;

    std::string get_val_by_offset(size_t offset) const;

    uint64_t get_trx_id_by_offset(size_t offset) const;

    int adjust_idx_by_trx_id(size_t index, uint64_t trx_id);

    int compare_key_by_offset(size_t offset, const std::string &target_key) const;

    bool is_same_key(size_t index, const std::string &compare_key) const;

private:
    friend class BlockIterator;

    std::vector<uint8_t> data;
    std::vector<uint16_t> offsets;
    size_t capacity;
};
} // LOG STRUCTURED MERGE TREE
