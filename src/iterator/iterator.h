#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

namespace LSMT {
enum class IteratorType {
    HeapIterator,
    SkiplistIterator,
    MemtableIterator,
    BlockIterator,
    SSTableIterator,
    ConcatIterator,
    LevelIterator,
    TwoMergeIterator,
};

class BaseIterator {
public:
    using IteratorItem = std::pair<std::string, std::string>;

    virtual IteratorItem operator*() const = 0;
    
    virtual BaseIterator& operator++() = 0;

    virtual bool operator==(const BaseIterator& other) const = 0;

    virtual bool operator!=(const BaseIterator& other) const = 0;

    virtual IteratorType get_iterator_type() const = 0;

    virtual uint64_t get_trx_id() const = 0;

    virtual bool is_end() const = 0;

    virtual bool is_vld() const = 0;
};

struct Item {
    std::string key;
    std::string val;
    uint64_t trx_id;
    int index;
    int level;

    Item() = default;
    
    Item(std::string key, std::string val, int index, int level, uint64_t trx_id);
    
    bool operator<(const Item &other) const;
    
    bool operator>(const Item &other) const;
    
    bool operator==(const Item &other) const;
    
    bool operator!=(const Item &other) const;
};

class HeapIterator : public BaseIterator {
    friend class SSTIterator;

public:
    HeapIterator() = default;

    HeapIterator(std::vector<Item> items, uint64_t max_trx_id);

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
} // LOG STRUCTURED MERGE TREE
