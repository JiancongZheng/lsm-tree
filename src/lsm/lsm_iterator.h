#pragma once

#include <memory>
#include <optional>
#include <shared_mutex>
#include <vector>


#include "sst/sst.h"
#include "sst/sst_iterator.h"

namespace LSMT {
class LSMTEngine;

class ConcatIterator : public BaseIterator {
public:
    ConcatIterator(std::vector<std::shared_ptr<SST>> ssts, uint64_t trx_id);

    IteratorItem* operator->() const;

    virtual IteratorItem operator*() const override;

    virtual BaseIterator &operator++() override;

    virtual bool operator==(const BaseIterator &other) const override;

    virtual bool operator!=(const BaseIterator &other) const override;

    virtual IteratorType get_iterator_type() const override;

    virtual uint64_t get_trx_id() const override;

    virtual bool is_end() const override;

    virtual bool is_vld() const override;
    
    std::string get_key();

    std::string get_val();

private:
    SSTIterator sst_iter;
    size_t curr_index;
    std::vector<std::shared_ptr<SST>> ssts;
    uint64_t max_trx_id;
};

class TwoMergeIterator : public BaseIterator {
public:
    TwoMergeIterator();

    TwoMergeIterator(std::shared_ptr<BaseIterator> it_new, std::shared_ptr<BaseIterator> it_old, uint64_t max_trx_id);

    IteratorItem* operator->() const;

    virtual IteratorItem operator*() const override;

    virtual BaseIterator &operator++() override;

    virtual bool operator==(const BaseIterator &other) const override;

    virtual bool operator!=(const BaseIterator &other) const override;

    virtual IteratorType get_iterator_type() const override;

    virtual uint64_t get_trx_id() const override;

    virtual bool is_end() const override;

    virtual bool is_vld() const override;

    bool choose_iter_new();

    void skip_iter_old();

    void skip_by_trx_id();

    void update_current() const;

private:
    std::shared_ptr<BaseIterator> iter_new;
    std::shared_ptr<BaseIterator> iter_old;
    bool choose_new = false;
    mutable std::shared_ptr<IteratorItem> current;
    uint64_t max_trx_id = 0;
};

class LevelIterator : public BaseIterator {
public:
    LevelIterator() = default;

    LevelIterator(std::shared_ptr<LSMTEngine> engine, uint64_t max_trx_id);

    IteratorItem* operator->() const;

    virtual IteratorItem operator*() const override;

    virtual BaseIterator &operator++() override;

    virtual bool operator==(const BaseIterator &other) const override;

    virtual bool operator!=(const BaseIterator &other) const override;

    virtual IteratorType get_iterator_type() const override;

    virtual uint64_t get_trx_id() const override;

    virtual bool is_end() const override;

    virtual bool is_vld() const override;

private:
    void update_current() const;

    std::pair<size_t, std::string> get_min_key_index() const;

    void skip_target_key(const std::string &key);

private:
    std::shared_ptr<LSMTEngine> engine;
    std::vector<std::shared_ptr<BaseIterator>> iters;
    size_t iter_index;
    uint64_t max_trx_id;
    mutable std::optional<IteratorItem> current;
    std::shared_lock<std::shared_mutex> rd_lock;
};

} // LOG STRUCTURED MERGE TREE