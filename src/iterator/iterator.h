#pragma once

#include <cstdint>
#include <memory>
#include <queue>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace LSMT {
enum class IteratorType {
    SkiplistIterator,
    MemtableIterator,
    HeapIterator,
    SSTableIterator,
    ConcactIterator,
    LevelIterator,
    TwoMergeIterator,
};

class BaseIterator {
public:
    using IteratorItem = std::pair<std::string, std::string>;

    virtual BaseIterator& operator++() = 0;

    virtual bool operator==(const BaseIterator& other) const = 0;

    virtual bool operator!=(const BaseIterator& other) const = 0;

    virtual IteratorItem operator*() const = 0;

    virtual IteratorType get_iterator_type() const = 0;

    virtual bool is_end() const = 0;

    virtual bool is_valid() const = 0;
};
} // LOG STRUCTURED MERGE TREE
