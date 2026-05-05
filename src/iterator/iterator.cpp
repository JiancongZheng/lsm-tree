#include "iterator.h"

namespace LSMT {
Item::Item(std::string key, std::string val, int index, int level, uint64_t trx_id)
: key(key), val(val), index(index), level(level), trx_id(trx_id) { }

bool Item::operator<(const Item &other) const {
    if (key != other.key) {
        return key < other.key;
    }
    return trx_id > other.trx_id || level < other.level  || index < other.index;
}

bool Item::operator>(const Item &other) const {
    if (key != other.key) {
        return key > other.key;
    }
    return trx_id < other.trx_id || level > other.level  || index > other.index;
}

bool Item::operator==(const Item &other) const {
    return key == other.key && index == other.index;
}

bool Item::operator!=(const Item &other) const {
    return key != other.key || index != other.index;
}

HeapIterator::HeapIterator(std::vector<Item> items, uint64_t max_trx_id) : max_trx_id(max_trx_id) {
    for (const auto& item : items) {
        pqueue.push(item);
    }
    while (!check_item_valid()) { skip_invld_items(); }
}

BaseIterator::IteratorItem* HeapIterator::operator->() const {
    update_current();
    return current.get();
}

BaseIterator::IteratorItem HeapIterator::operator*() const {
    update_current();
    return std::make_pair(pqueue.top().key, pqueue.top().val);
}

BaseIterator &HeapIterator::operator++() {
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

bool HeapIterator::operator==(const BaseIterator &other) const {
    if (other.get_iterator_type() != IteratorType::HeapIterator) {
        return false;
    }
    const HeapIterator &other_heap = dynamic_cast<const HeapIterator&>(other);
    if (pqueue.empty() && other_heap.pqueue.empty()) {
        return true;
    }
    if (pqueue.empty() || other_heap.pqueue.empty()) {
        return false;
    }
    return pqueue.top().key == other_heap.pqueue.top().key &&
           pqueue.top().val == other_heap.pqueue.top().val;
}

bool HeapIterator::operator!=(const BaseIterator &other) const {
    return !(*this == other);
}

IteratorType HeapIterator::get_iterator_type() const {
    return IteratorType::HeapIterator;
}

uint64_t HeapIterator::get_trx_id() const {
    return max_trx_id;
}

bool HeapIterator::is_end() const {
    return pqueue.empty() == true;
}

bool HeapIterator::is_vld() const {
    return pqueue.empty() != true;
}

bool HeapIterator::check_item_valid() {
    if (pqueue.empty()) {
        return true;
    }
    if (max_trx_id == 0 || pqueue.top().trx_id < max_trx_id) {
        return pqueue.top().val.size() > 0;
    } else {
        return false;
    }
}

void HeapIterator::skip_invld_items() {
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

void HeapIterator::update_current() const {
    if (!pqueue.empty()) {
        current = std::make_shared<IteratorItem>(pqueue.top().key, pqueue.top().val);
    } else {
        current.reset();
    }
}
} // LOG STRUCTURED MERGE TREE