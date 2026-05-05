#include "memtable_iterator.h"

namespace LSMT {
MemtableIterator::MemtableIterator(std::vector<Item> items, uint64_t max_trx_id) : max_trx_id(max_trx_id) {
    for (const auto& item : items) {
        pqueue.push(item);
    }
    while (!check_item_valid()) { skip_invld_items(); }
}

BaseIterator::IteratorItem* MemtableIterator::operator->() const {
    update_current();
    return current.get();
}

BaseIterator::IteratorItem MemtableIterator::operator*() const {
    update_current();
    return std::make_pair(pqueue.top().key, pqueue.top().val);
}

BaseIterator &MemtableIterator::operator++() {
    if (pqueue.empty()) {
        return *this;
    }
    auto old_item = pqueue.top();
    while (!pqueue.empty() && pqueue.top().key == old_item.key) {
        pqueue.pop();
    }
    while (!check_item_valid()) { 
        skip_invld_items(); 
    }
    return *this;
}

bool MemtableIterator::operator==(const BaseIterator &other) const {
    if (other.get_iterator_type() != IteratorType::MemtableIterator) {
        return false;
    }
    const MemtableIterator &other_memtable = dynamic_cast<const MemtableIterator&>(other);
    if (pqueue.empty() && other_memtable.pqueue.empty()) {
        return true;
    }
    if (pqueue.empty() || other_memtable.pqueue.empty()) {
        return false;
    }
    return pqueue.top().key == other_memtable.pqueue.top().key &&
           pqueue.top().val == other_memtable.pqueue.top().val;
}

bool MemtableIterator::operator!=(const BaseIterator &other) const {
    return !(*this == other);
}

IteratorType MemtableIterator::get_iterator_type() const {
    return IteratorType::MemtableIterator;
}

uint64_t MemtableIterator::get_trx_id() const {
    return max_trx_id;
}

bool MemtableIterator::is_end() const {
    return pqueue.empty() == true;
}

bool MemtableIterator::is_vld() const {
    return pqueue.empty() != true;
}

bool MemtableIterator::check_item_valid() {
    if (pqueue.empty()) {
        return true;
    }
    if (max_trx_id == 0 || pqueue.top().trx_id < max_trx_id) {
        return pqueue.top().val.size() > 0;
    } else {
        return false;
    }
}

void MemtableIterator::skip_invld_items() {
    // 删除不可见元素和被删除元素
    while (max_trx_id != 0 && !pqueue.empty() && pqueue.top().trx_id > max_trx_id) {
        pqueue.pop();
    }
    while (!pqueue.empty() && pqueue.top().val.empty()) {
        auto del_key = pqueue.top().key;
        while (!pqueue.empty() && pqueue.top().key == del_key) {
            pqueue.pop();
        }
    }
}

void MemtableIterator::update_current() const {
    if (!pqueue.empty()) {
        current = std::make_shared<IteratorItem>(pqueue.top().key, pqueue.top().val);
    } else {
        current.reset();
    }
}
}  // LOG STRUCT MERGE TREE