#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "iterator/iterator.h"

namespace LSMT {
class MemtableIterator : public BaseIterator {
public:
    MemtableIterator() = default;

    MemtableIterator(std::vector<Item> items, uint64_t max_trx_id);

    IteratorItem* operator->() const;

    virtual IteratorItem operator*() const override;

    BaseIterator &operator++() override;

    BaseIterator operator++(int) = delete;

    virtual bool operator==(const BaseIterator &other) const override;

    virtual bool operator!=(const BaseIterator &other) const override;

    virtual IteratorType get_iterator_type() const override;

    virtual uint64_t get_trx_id() const override;

    virtual bool is_end() const override;

    virtual bool is_vld() const override;

private:
    bool check_item_valid();

    void skip_invld_items();

    void update_current() const;

private:
    std::priority_queue<Item, std::vector<Item>, std::greater<Item>> pqueue;
    mutable std::shared_ptr<IteratorItem> current;
    uint64_t max_trx_id = 0;
};
}  // LOG STRUCTURED MERGE TREE