#include "skiplist.h"
#include "skiplist_iterator.h"

namespace LSMT {
SkipListIterator::SkipListIterator() : current(nullptr), rd_lock(nullptr) { }

SkipListIterator::SkipListIterator(std::shared_ptr<SkipListNode> node): current(node) { }

BaseIterator::IteratorItem SkipListIterator::operator*() const {
    if (!current) { 
        throw std::runtime_error("SkipList Iterator Error: dereferencing end iterator");
    }
    return {current->key, current->val};
}

BaseIterator& SkipListIterator::operator++() {
    if (current) { current = current->next[0]; }
    return *this;
}

bool SkipListIterator::operator==(const BaseIterator &other) const {
    if (other.get_iterator_type() != IteratorType::SkiplistIterator) { 
        return false; 
    }
    auto other_skiplist = dynamic_cast<const SkipListIterator&>(other);
    return current == other_skiplist.current;
}

bool SkipListIterator::operator!=(const BaseIterator &other) const {
    return !(*this == other);
}


IteratorType SkipListIterator::get_iterator_type() const {
    return IteratorType::SkiplistIterator;
}

bool SkipListIterator::is_end() const { 
    return current == nullptr; 
}

bool SkipListIterator::is_vld() const { 
    return current != nullptr && !current->key.empty(); 
}

std::string SkipListIterator::get_key() const { 
    return current->key; 
}

std::string SkipListIterator::get_val() const { 
    return current->val; 
}

uint64_t SkipListIterator::get_trx_id() const { 
    return current->trx_id; 
}
} // LOG STRUCTURED MERGE TREE