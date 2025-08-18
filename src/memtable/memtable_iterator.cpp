#include "memtable_iterator.h"

namespace LSMT {
bool operator<(const Item &item1, const Item &item2) {
    if (item1.key != item2.key) {
        return item1.key < item2.key;
    }
    return item1.trx_id > item2.trx_id || item1.level  < item2.level  || item1.index  < item2.index;
    }

bool operator>(const Item &item1, const Item &item2) {
    if (item1.key != item2.key) {
        return item1.key > item2.key;
    }
    return item1.trx_id < item2.trx_id || item1.level  > item2.level  || item1.index  > item2.index;
}

bool operator==(const Item &item1, const Item &item2) {
    return item1.key == item2.key && item1.index == item2.index;
}

bool operator!=(const Item &item1, const Item &item2) {
    return item1.key != item2.key || item1.index != item2.index;
}

MemtableIterator::MemtableIterator(std::vector<Item> items, uint64_t max_trx_id) : max_trx_id(max_trx_id) {
    for (const auto& item : items) {
        pqueue.push(item);
    }
    while (!check_item_valid()) { skip_invld_items(); }
}

bool MemtableIterator::check_item_valid() {
    if (pqueue.empty()) {
        return true;
    }
    if (max_trx_id ==0 || pqueue.top().trx_id < max_trx_id) {
        return pqueue.top().val.size() > 0;
    } else {
        return false;
    }
}

void MemtableIterator::skip_invld_items() {
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

MemtableIterator::IteratorItem* MemtableIterator::operator->() const {
    update_current();
    return current.get();
}

MemtableIterator::IteratorItem MemtableIterator::operator*() const {
    return std::make_pair(pqueue.top().key, pqueue.top().val);
}

BaseIterator& MemtableIterator::operator++() {
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
    auto other_heap = dynamic_cast<const MemtableIterator&>(other);
    if (pqueue.empty() && other_heap.pqueue.empty()) {
        return true;
    }
    if (pqueue.empty() || other_heap.pqueue.empty()) {
        return false;
    }
    return pqueue.top().key == other_heap.pqueue.top().key &&
           pqueue.top().val == other_heap.pqueue.top().val;
}

bool MemtableIterator::operator!=(const BaseIterator &other) const {
    if (other.get_iterator_type() != IteratorType::MemtableIterator) {
        return true;
    }
    auto other_heap = dynamic_cast<const MemtableIterator&>(other);
    if (pqueue.empty() && other_heap.pqueue.empty()) {
        return false;
    }
    if (pqueue.empty() || other_heap.pqueue.empty()) {
        return true;
    }
    return pqueue.top().key != other_heap.pqueue.top().key || 
           pqueue.top().val != other_heap.pqueue.top().val;
}

IteratorType MemtableIterator::get_iterator_type() const {
    return IteratorType::MemtableIterator;
}

bool MemtableIterator::is_end() const {
    return pqueue.empty() == true;
}

bool MemtableIterator::is_valid() const {
    return pqueue.empty() != true;
}

uint64_t MemtableIterator::get_trx_id() const {
    return max_trx_id;
}
}  // LOG STRUCT MERGE TREE