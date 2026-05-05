#include "sst.h"
#include "sst_iterator.h"

namespace LSMT {
SSTIterator::SSTIterator(std::shared_ptr<SST> sst, uint64_t trx_id) 
: sst(sst), block_id(0), block_it(nullptr), max_trx_id(trx_id) {
    if (sst == nullptr || sst->get_block_number() == 0) {
        return;
    }
    block_it = std::make_shared<BlockIterator>(sst->get_block(block_id), 0, trx_id);
}

SSTIterator::SSTIterator(std::shared_ptr<SST> sst, const std::string &key, uint64_t trx_id)
: sst(sst), block_id(0), block_it(nullptr), max_trx_id(trx_id) {
    if (sst == nullptr || sst->get_block_number() == 0) {
        return;
    }
    try {
        block_id = sst->get_block_id(key);
        if (block_id == -1 || block_id >= sst->get_block_number()) {
            block_id = sst->get_block_number();
            block_it = nullptr;
            return;
        }
        block_it = std::make_shared<BlockIterator>(sst->get_block(block_id), key, trx_id);
        if (block_it->is_end()) {
            block_id = sst->get_block_number();
            block_it = nullptr;
            return;
        }
    } catch (const std::exception &) {
        block_it = nullptr;
        return;
    }
}

BaseIterator::IteratorItem* SSTIterator::operator->() const {
    update_current();
    return &(cached_value.value());
}
    
BaseIterator::IteratorItem SSTIterator::operator*() const {
    update_current();
    return cached_value.value();
}

SSTIterator::BaseIterator &SSTIterator::operator++() {
    if (block_it == nullptr) {
        return *this;
    }
    ++(*block_it);
    if (block_it->is_end()) {
        block_id++;
        if (block_id < sst->get_block_number()) {
            block_it = std::make_shared<BlockIterator>(sst->get_block(block_id), 0, max_trx_id);
        } else {
            block_it = nullptr;
        }
    }
    return *this;
}

bool SSTIterator::operator==(const BaseIterator &other) const {
    if (other.get_iterator_type() != IteratorType::SSTableIterator) {
        return false;
    }
    const SSTIterator &other_sst_iter = dynamic_cast<const SSTIterator&>(other);
    if (sst != other_sst_iter.sst || block_id != other_sst_iter.block_id) {
        return false;
    }
    if (block_it == nullptr && other_sst_iter.block_it == nullptr) {
        return true;
    }
    if (block_it == nullptr || other_sst_iter.block_it == nullptr) {
        return false;
    }
    return *block_it == *(other_sst_iter.block_it);
}

bool SSTIterator::operator!=(const BaseIterator &other) const {
    return !(*this == other);
}

IteratorType SSTIterator::get_iterator_type() const {
    return IteratorType::SSTableIterator;
}

uint64_t SSTIterator::get_trx_id() const {
    return max_trx_id;
}

bool SSTIterator::is_end() const {
    return block_it == nullptr;
}

bool SSTIterator::is_vld() const {
    return block_it && !block_it->is_end() && block_id < sst->get_block_number();
}

void SSTIterator::update_current() const {
    if (block_it != nullptr && !block_it->is_end()) {
        cached_value = *(*block_it);
    } else {
        cached_value = std::nullopt;
    }
}

std::string SSTIterator::get_key() {
    update_current();
    if (cached_value.has_value()) {
        return cached_value->first;
    } else {
        throw std::out_of_range("SSTIterator is Invalid");
    }
}

std::string SSTIterator::get_val() {
    update_current();
    if (cached_value.has_value()) {
        return cached_value->second;
    } else {
        throw std::out_of_range("SSTIterator is Invalid");
    }
}

std::pair<HeapIterator, HeapIterator>
SSTIterator::merge_sst_iterator(std::vector<SSTIterator> iters, uint64_t trx_id) {
    if (iters.empty()) {
        return std::make_pair(HeapIterator(), HeapIterator());
    }
    HeapIterator heap_beg;
    HeapIterator heap_end;
    for (auto iter : iters) {
        if (iter.is_vld() && !iter.is_end()) {
            heap_beg.pqueue.emplace(iter.get_key(), iter.get_val(), -iter.sst->get_sst_id(), 0, iter.get_trx_id());
        }
    }
    return std::make_pair(heap_beg, heap_end);
}
} // LOG STRUCTURED MERGE TREE