#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "../iterator/iterator.h"

namespace LSMT {

struct Item {
    std::string key;
    std::string val;
    uint64_t trx_id;
    int index;
    int level;

    Item() = default;
    
    Item(std::string key, std::string val, int index, int level, uint64_t trx_id) 
    : key(key), val(val), trx_id(trx_id), index(index), level(level) { }
};

bool operator<(const Item &item1, const Item &item2);

bool operator>(const Item &item1, const Item &item2);

bool operator==(const Item &item1, const Item &item2);

bool operator!=(const Item &item1, const Item &item2);

class MemtableIterator : public BaseIterator {
public:
    MemtableIterator() = default;

    MemtableIterator(std::vector<Item> items, uint64_t max_trx_id);

    virtual BaseIterator& operator++() override;

    virtual bool operator==(const BaseIterator &other) const override;

    virtual bool operator!=(const BaseIterator &other) const override;

    virtual IteratorItem operator*() const override;

    virtual IteratorType get_iterator_type() const override;

    virtual bool is_end() const override;

    virtual bool is_valid() const override;
    
    IteratorItem* operator->() const;

    virtual uint64_t get_trx_id() const;

private:
    bool check_item_valid();

    void skip_invld_items();

    void update_current() const;

private:
    // friend class SSTableIterator;

    std::priority_queue<Item, std::vector<Item>, std::greater<Item>> pqueue;
    mutable std::shared_ptr<IteratorItem> current;
    uint64_t max_trx_id;
};
}  // LOG STRUCT MERGE TREE