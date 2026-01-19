#include "block.h"
#include "block_iterator.h"

namespace LSMT {
BlockIterator::BlockIterator() : block(nullptr), curr_index(0), trx_id(0) { }

BlockIterator::BlockIterator(std::shared_ptr<Block> blk, size_t index, uint64_t trx_id) 
    : block(blk), curr_index(index), trx_id(trx_id), cached_kvpair(std::nullopt) {
    skip_by_trx_id();
    update_current();
}

BlockIterator::BlockIterator(std::shared_ptr<Block> blk, const std::string &key, uint64_t trx_id) 
    : block(blk), trx_id(trx_id), cached_kvpair(std::nullopt) {
    auto index = blk->get_idx_binary(key, trx_id);
    if (index.has_value()) {
        curr_index = index.value();
    } else {
        curr_index = blk->offsets.size();
    }
}

const std::pair<std::string, std::string>* BlockIterator::operator->() const {
    return &(cached_kvpair.value());
}

BlockIterator &BlockIterator::operator++() {
    Entry entry = block->get_entry_by_offset(block->get_offset(curr_index));
    while (block && curr_index < block->offsets.size() 
        && entry.key == block->get_entry_by_offset(block->get_offset(curr_index)).key) {
        ++curr_index;
    }
    skip_by_trx_id();
    update_current();
    return *this;
}

bool BlockIterator::operator==(const BlockIterator &other) const {
    if (block == nullptr && other.block == nullptr) {
        return true;
    }
    if (block == nullptr || other.block == nullptr) {
        return false;
    }
    return block == other.block && curr_index == other.curr_index;
}

bool BlockIterator::operator!=(const BlockIterator &other) const {
    if (block == nullptr && other.block == nullptr) {
        return false;
    }
    if (block == nullptr || other.block == nullptr) {
        return true;
    }
    return block != other.block || curr_index != other.curr_index;
}

std::pair<std::string, std::string> BlockIterator::operator*() const {
    if (!block || curr_index >= block->offsets.size()) {
        throw std::out_of_range("Iterator out of offset range");
    }
    if (!cached_kvpair.has_value()) {
        size_t offset = block->get_offset(curr_index);
        cached_kvpair = std::make_pair(block->get_key_by_offset(offset), block->get_val_by_offset(offset));
    }
    return cached_kvpair.value();
}

bool BlockIterator::is_end() {
    return curr_index = block->offsets.size();
}

void BlockIterator::update_current() const {
    if (curr_index < block->offsets.size()) {
        size_t offset = block->get_offset(curr_index);
        cached_kvpair = std::make_pair(block->get_key_by_offset(offset), block->get_val_by_offset(offset));
    }
}

void BlockIterator::skip_by_trx_id() {
    while (trx_id != 0 && curr_index < block->offsets.size()) {
        size_t offset = block->get_offset(curr_index);
        if (block->get_trx_id_by_offset(offset) <= trx_id) {
            break;
        }
        ++curr_index;
    }
}
} // LOG STRUCTURED MERGE TREE