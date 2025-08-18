#pragma once

#include <cstdint>
#include <cstddef>
#include <memory>
#include <shared_mutex>
#include <string>

#include "../iterator/iterator.h"

namespace LSMT {
struct SkipListNode;

class SkipListIterator : public BaseIterator {
public:
    SkipListIterator();

    SkipListIterator(std::shared_ptr<SkipListNode> node);

    virtual BaseIterator& operator++() override;

    virtual bool operator==(const BaseIterator &other) const override;

    virtual bool operator!=(const BaseIterator &other) const override;

    virtual IteratorItem operator*() const override;

    virtual IteratorType get_iterator_type() const override;

    virtual bool is_end() const override;

    virtual bool is_valid() const override;

    std::string get_key() const;

    std::string get_val() const;

    uint64_t get_trx_id() const;

private:
    std::shared_ptr<SkipListNode> current;
    std::shared_ptr<std::shared_lock<std::shared_mutex>> rd_lock;
};
} // LOG STRUCTURED MERGE TREE