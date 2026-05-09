#include "lsm_engine.h"
#include "lsm_iterator.h"

namespace LSMT {
ConcatIterator::ConcatIterator(std::vector<std::shared_ptr<SST>> ssts, uint64_t trx_id)
    : ssts(ssts), sst_iter(nullptr, trx_id), curr_index(0), max_trx_id(trx_id) {
    if (!this->ssts.empty()) {
        sst_iter = ssts[0]->begin(max_trx_id);
    }
}

BaseIterator::IteratorItem* ConcatIterator::operator->() const {
    return sst_iter.operator->();
}

BaseIterator::IteratorItem ConcatIterator::operator*() const {
    return *sst_iter;
}

BaseIterator &ConcatIterator::operator++() {
    ++sst_iter;
    if (sst_iter.is_end() || !sst_iter.is_vld()) {
        curr_index++;
        if (curr_index < ssts.size()) {
            sst_iter = ssts[curr_index]->begin(max_trx_id);
        } else {
            sst_iter = SSTIterator(nullptr, max_trx_id);
        }
    }
    return *this;
}

bool ConcatIterator::operator==(const BaseIterator &other) const {
    if (other.get_iterator_type() != IteratorType::ConcatIterator) {
        return false;
    }
    auto other_iter = dynamic_cast<const ConcatIterator &>(other);
    return this->sst_iter == other_iter.sst_iter;
}

bool ConcatIterator::operator!=(const BaseIterator &other) const {
    return !(*this == other);
}

IteratorType ConcatIterator::get_iterator_type() const {
    return IteratorType::ConcatIterator;
}

uint64_t ConcatIterator::get_trx_id() const {
    return max_trx_id;
}

bool ConcatIterator::is_end() const {
    return sst_iter.is_end() || !sst_iter.is_vld();
}

bool ConcatIterator::is_vld() const {
    return !sst_iter.is_end() && sst_iter.is_vld();
}

std::string ConcatIterator::get_key() {
    return sst_iter.get_key();
}

std::string ConcatIterator::get_val() {
    return sst_iter.get_val();
}



TwoMergeIterator::TwoMergeIterator() { }

TwoMergeIterator::TwoMergeIterator(std::shared_ptr<BaseIterator> it_new, std::shared_ptr<BaseIterator> it_old, 
        uint64_t max_trx_id)
    : iter_new(it_new), iter_old(it_old), max_trx_id(max_trx_id) {
    skip_by_trx_id();
    skip_iter_old();
    choose_new = choose_iter_new();
}


bool TwoMergeIterator::choose_iter_new() {
    if (iter_new->is_end()) {
        return false;
    }
    if (iter_old->is_end()) {
        return true;
    }

    auto new_key = iter_new->operator*().first;
    auto old_key = iter_old->operator*().first;
    if (new_key != old_key) {
        return new_key < old_key;
    } else {
    }

    return true;
}

void TwoMergeIterator::skip_iter_old() {
    if (!iter_new->is_end() && !iter_old->is_end() && (**iter_new).first == (**iter_old).first) {
        ++(*iter_old);
    }
}

void TwoMergeIterator::skip_by_trx_id() {
    if (max_trx_id != 0) {
        while (!iter_new->is_end() && iter_new->get_trx_id() > max_trx_id) {
            ++(*iter_new);
        }
        while (!iter_old->is_end() && iter_old->get_trx_id() > max_trx_id) {
            ++(*iter_old);
        }
    }
}

void TwoMergeIterator::update_current() const {
    if (choose_new) {
        current = std::make_shared<IteratorItem>(*(*iter_new));
    } else {
        current = std::make_shared<IteratorItem>(*(*iter_old));
    }
}

BaseIterator::IteratorItem* TwoMergeIterator::operator->() const {
    update_current();
    return &(*current);
}

BaseIterator::IteratorItem TwoMergeIterator::operator*() const {
    update_current();
    return *current;
}

BaseIterator &TwoMergeIterator::operator++() {
    if (choose_new) {
        ++(*iter_new);
    } else {
        ++(*iter_old);
    }
    skip_by_trx_id();
    skip_iter_old();
    choose_new = choose_iter_new();
    return *this;
}

bool TwoMergeIterator::operator==(const BaseIterator &other) const {
    if (other.get_iterator_type() != IteratorType::TwoMergeIterator) {
        return false;
    }
    auto other_iter = dynamic_cast<const TwoMergeIterator &>(other);
    if (this->is_end() && other_iter.is_end()) {
        return true;
    }
    if (this->is_end() || other_iter.is_end()) {
        return false;
    }
    return this->iter_new == other_iter.iter_new && this->iter_old == other_iter.iter_old 
           && choose_new == other_iter.choose_new;
}

bool TwoMergeIterator::operator!=(const BaseIterator &other) const {
    return !(*this == other);
}

IteratorType TwoMergeIterator::get_iterator_type() const {
    return IteratorType::TwoMergeIterator;
}

uint64_t TwoMergeIterator::get_trx_id() const {
    return max_trx_id;
}

//! is_end和is_vld函数中什么时候会存在iter_new或iter_old为nullptr的情况？
bool TwoMergeIterator::is_end() const {
    if (iter_new == nullptr && iter_old == nullptr) {
        return true;
    }
    if (iter_new == nullptr) {
        return iter_old->is_end();
    }
    if (iter_old == nullptr) {
        return iter_new->is_end();
    }
    return iter_new->is_end() && iter_old->is_end();
}

bool TwoMergeIterator::is_vld() const {
    if (iter_new == nullptr && iter_old == nullptr) {
        return false;
    }
    if (iter_new == nullptr) {
        return iter_old->is_vld();
    }
    if (iter_old == nullptr) {
        return iter_new->is_vld();
    }
    return iter_new->is_vld() || iter_old->is_vld();
}



LevelIterator::LevelIterator(std::shared_ptr<LSMTEngine> engine, uint64_t max_trx_id)
: engine(engine), max_trx_id(max_trx_id), rd_lock(engine->lsmt_mutex) {
    // 获取Memtable的迭代器指针
    auto memtable_it = engine->memtable.begin(max_trx_id);
    auto memtable_it_ptr = std::make_shared<HeapIterator>(memtable_it);
    iters.push_back(memtable_it_ptr);

    // 获取SSTable的迭代器指针
    for (auto &[level, sst_indexes] : engine->sst_indexes) {
        if (level == 0) {
            std::vector<Item> items;
            for (auto &sst_index : engine->sst_indexes[0]) {
                auto sst = engine->ssts[sst_index];
                for (auto it = sst->begin(max_trx_id); it.is_vld() && !it.is_end(); ++it) {
                    if (max_trx_id != 0 && it.get_trx_id() > max_trx_id) {
                        continue;
                    }
                    items.emplace_back(it.get_key(), it.get_val(), -sst_index, 0, it.get_trx_id());
                }
            }
            auto sstable_it_ptr = std::make_shared<HeapIterator>(items, max_trx_id);
            iters.push_back(sstable_it_ptr);
        } else {
            std::vector<std::shared_ptr<SST>> ssts;
            for (auto sst_index : sst_indexes) {
                ssts.push_back(engine->ssts[sst_index]);
            }
            auto sstable_it_ptr = std::make_shared<ConcatIterator>(ssts, max_trx_id);
            iters.push_back(sstable_it_ptr);
        }
    }

    while (!is_end()) {
        auto [min_index, _] = get_min_key_index();
        iter_index = min_index;
        update_current();
        // 空值表示当前key被删除了, 需要跳过当前key
        if (current->second.size() == 0) {
            skip_target_key(current->first);
        } else {
            break;
        }
    }
}

void LevelIterator::update_current() const {
    if (iters[iter_index]->is_vld() == false) {
        throw std::runtime_error("LevelIterator has no valid item to dereference");
    }
    current = std::optional<IteratorItem>(**iters[iter_index]);
}

std::pair<size_t, std::string> LevelIterator::get_min_key_index() const {
    size_t min_idx = 0;
    std::string min_key = "";
    for (size_t idx = 0; idx < iters.size(); ++idx) {
        if (!iters[idx]->is_vld()) {
            continue;
        } else if (min_key == "") {
            min_key = (**iters[idx]).first;
            min_idx = idx;
        } else if (min_key > (**iters[idx]).first) {
            min_key = (**iters[idx]).first;
            min_idx = idx;
        } else if (min_key == (**iters[idx]).first) {
            min_idx = (**iters[idx]).first < min_key ? idx : min_idx;
        } else {
            // Nothing to do
        }
    }
    return {min_idx, min_key};
}

void LevelIterator::skip_target_key(const std::string &key) {
    for (auto &iter : iters) {
        while (iter->is_vld() && (**iter).first == key) {
            ++(*iter);
        }
    }
}

BaseIterator::IteratorItem* LevelIterator::operator->() const {
    update_current();
    return &(*current);
}

BaseIterator::IteratorItem LevelIterator::operator*() const {
    update_current();
    return *current;
}

BaseIterator &LevelIterator::operator++() {
    skip_target_key(current->first);
    while (is_end() == false) {
        auto [min_index, _] = get_min_key_index();
        iter_index = min_index;
        update_current();
        if (current->second.size() == 0) {
            skip_target_key(current->first);
        } else {
            break;
        }
    }
    return *this;
}

bool LevelIterator::operator==(const BaseIterator &other) const {
    if (other.get_iterator_type() != IteratorType::LevelIterator) {
        return false;
    }
    if (this->is_vld() && other.is_vld()) {
        return current->first == (*other).first && current->second == (*other).second;
    }
    return !this->is_vld() && !other.is_vld();
}

bool LevelIterator::operator!=(const BaseIterator &other) const {
    return !(*this == other);
}

IteratorType LevelIterator::get_iterator_type() const {
    return IteratorType::LevelIterator;
}

uint64_t LevelIterator::get_trx_id() const {
    return max_trx_id;
}

bool LevelIterator::is_end() const {
    return std::all_of(iters.begin(), iters.end(), [](const auto &it) { 
        return it->is_end(); 
    });
}

bool LevelIterator::is_vld() const {
    return std::any_of(iters.begin(), iters.end(), [](const auto &it) { 
        return it->is_vld(); 
    });
}

} // LOG STRUCTURED MERGE TREE