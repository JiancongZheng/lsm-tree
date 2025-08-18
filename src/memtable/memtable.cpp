#include "./memtable.h"

namespace LSMT {
MemTable::MemTable() : frozen_bytes(0) {
    active_table = std::make_shared<SkipList>();
}

void MemTable::put(const std::string &key, const std::string &val, uint64_t trx_id) {
    std::unique_lock<std::shared_mutex> act_lock(active_mutex);
    active_table->put(key, val, trx_id);
    if (active_table->get_size() > 10000) {
        std::unique_lock<std::shared_mutex> frz_lock(frozen_mutex);
        freeze_active_table();
    }
}

void MemTable::put(const std::vector<std::pair<std::string, std::string>> &kv_pairs, uint64_t trx_id) {
    std::unique_lock<std::shared_mutex> act_lock(active_mutex);
    for (const auto& [key, val] : kv_pairs) {
        active_table->put(key, val, trx_id);
    }
    if (active_table->get_size() > 10000) {
        std::unique_lock<std::shared_mutex> frz_lock(frozen_mutex);
        freeze_active_table();
    }
}

void MemTable::remove(const std::string &key, uint64_t trx_id) {
    std::unique_lock<std::shared_mutex> act_lock(active_mutex);
    active_table->put(key, "", trx_id);
    if (active_table->get_size() > 10000) {
        std::unique_lock<std::shared_mutex> frz_lock(frozen_mutex);
        freeze_active_table();
    }
}

void MemTable::remove(const std::vector<std::string> &keys, uint64_t trx_id) {
    std::unique_lock<std::shared_mutex> act_lock(active_mutex);
    for (auto &key : keys) {
        active_table->put(key, "", trx_id);
    }
    if (active_table->get_size() > 10000) {
        std::unique_lock<std::shared_mutex> frz_lock(frozen_mutex);
        freeze_active_table();
    }
}

bool MemTable::get_active(const std::string &key, uint64_t trx_id, SkipListIterator &it) {
    it = active_table->get(key, trx_id);
    return it.is_valid();
}

bool MemTable::get_frozen(const std::string &key, uint64_t trx_id, SkipListIterator &it) {
    for (auto &table : frozen_table) {
        it = table->get(key, trx_id);
        if (it.is_valid()) { return true; }
    }
    return false;
}

SkipListIterator MemTable::get(const std::string &key, uint64_t trx_id) {
    std::shared_lock<std::shared_mutex> act_lock(active_mutex);
    SkipListIterator iterator = SkipListIterator();

    if (get_active(key, trx_id, iterator) == true) {
        return iterator;
    }
    act_lock.unlock();
    std::shared_lock<std::shared_mutex> frz_lock(frozen_mutex);
    if (get_frozen(key, trx_id, iterator) == true) {
        return iterator;
    }
    return SkipListIterator();
}

std::vector<std::pair<std::string, std::optional<std::string>>>
MemTable::get(const std::vector<std::string> &keys, uint64_t trx_id) {
    std::vector<std::pair<std::string, std::optional<std::string>>> items;

    std::shared_lock<std::shared_mutex> act_lock(active_mutex);

    for (size_t idx = 0; idx < keys.size(); ++idx) {
        auto iterator = SkipListIterator(); 
        if (get_active(keys[idx], trx_id, iterator)) {
            items.emplace_back(keys[idx], iterator.get_val());
        } else {
            items.emplace_back(keys[idx], std::nullopt);
        }
    }
    if (!std::any_of(items.begin(), items.end(), [](const auto &e) { return !e.second.has_value(); })) {
        return items;
    }

    act_lock.unlock();
    std::shared_lock<std::shared_mutex> frz_lock(frozen_mutex);

    for (size_t idx = 0; idx < keys.size(); ++idx) {
        if (items[idx].second.has_value()) continue;
        auto iterator = SkipListIterator();
        if (get_frozen(keys[idx], trx_id, iterator)) {
            items[idx] = std::make_pair(keys[idx], iterator.get_val());
        } else {
            items[idx] = std::make_pair(keys[idx], std::nullopt);
        }
    }
    return items;
}

MemtableIterator MemTable::begin(uint64_t trx_id) {
    std::shared_lock<std::shared_mutex> act_lock(active_mutex);
    std::shared_lock<std::shared_mutex> frz_lock(frozen_mutex);
    std::vector<Item> items;

    for (auto it = active_table->begin(); it != active_table->end(); ++it) {
        if (trx_id != 0 && it.get_trx_id() > trx_id) 
            continue;
        items.emplace_back(it.get_key(), it.get_val(), 0, 0, it.get_trx_id());
    }
    for (size_t idx = 0; idx < frozen_table.size(); ++idx) {
        auto table = frozen_table[idx];
        for (auto it = table->begin(); it != table->end(); ++it) {
            if (trx_id != 0 && it.get_trx_id() > trx_id)
                continue;
            items.emplace_back(it.get_key(), it.get_val(), idx + 1, 0, it.get_trx_id());
        }
    }

    return MemtableIterator(items, trx_id);
}

MemtableIterator MemTable::end() {
    std::shared_lock<std::shared_mutex> act_lock(active_mutex);
    std::shared_lock<std::shared_mutex> frz_lock(frozen_mutex);
    return MemtableIterator();
}

MemtableIterator MemTable::iters_preffix(const std::string &preffix, uint64_t trx_id) {
    std::shared_lock<std::shared_mutex> act_lock(active_mutex);
    std::shared_lock<std::shared_mutex> frz_lock(frozen_mutex);
    std::vector<Item> items;

    for (auto it = active_table->begin_preffix(preffix); it != active_table->end_preffix(preffix); ++it) {
        if (trx_id != 0 && it.get_trx_id() > trx_id) {
            continue;
        }
        if (!items.empty() && items.back().key == it.get_key()) {
            continue;
        }
        items.emplace_back(it.get_key(), it.get_val(), 0, 0, it.get_trx_id());
    }

    for (int idx = 0; idx < frozen_table.size(); ++idx) {
        auto table = frozen_table[idx];
        for (auto it = table->begin_preffix(preffix); it != table->end_preffix(preffix); ++it) {
            if (trx_id != 0 && it.get_trx_id() > trx_id) {
                continue;
            }
            if (!items.empty() && items.back().key == it.get_key()) {
                continue;
            }
            items.emplace_back(it.get_key(), it.get_val(), idx + 1, 0, it.get_trx_id());
        }
    }
    return MemtableIterator(items, trx_id);
}


std::optional<std::pair<MemtableIterator, MemtableIterator>> 
MemTable::iters_monotony_predicate(uint64_t trx_id, std::function<int(const std::string&)> predicate) {
    std::shared_lock<std::shared_mutex> act_lock(active_mutex);
    std::shared_lock<std::shared_mutex> frz_lock(frozen_mutex);
    std::vector<Item> items;

    auto active_result = active_table->iters_monotony_predicate(predicate);
    if (active_result.has_value()) {
        auto [begin, end] = active_result.value();
        for (auto it = begin; it != end; ++it) {
            if (trx_id != 0 && it.get_trx_id() > trx_id) {
                continue;
            }
            if (!items.empty() && items.back().key == it.get_key()) {
                continue;
            }
            items.emplace_back(it.get_key(), it.get_val(), 0, 0, it.get_trx_id());
        }
    }

    for (size_t idx = 0; idx < frozen_table.size(); ++idx) {
        auto frozen_result = frozen_table[idx]->iters_monotony_predicate(predicate);
        if (frozen_result.has_value()) {
            auto [begin, end] = frozen_result.value();
            for (auto it = begin; it != end; ++it) {
                if (trx_id != 0 && it.get_trx_id() > trx_id) {
                    continue;
                }
                if (!items.empty() && items.back().key == it.get_key()) {
                    continue;
                }
                items.emplace_back(it.get_key(), it.get_val(), idx + 1, 0, it.get_trx_id());
            }
        }
    }

    if (items.empty()) {
        return std::nullopt;
    } else {
        return std::make_pair(MemtableIterator(items,trx_id), MemtableIterator());
    }
}


void MemTable::freeze_memtable() {
    std::shared_lock<std::shared_mutex> act_lock(active_mutex);
    std::shared_lock<std::shared_mutex> frz_lock(frozen_mutex);
    freeze_active_table();
}

void MemTable::freeze_active_table() {
    frozen_bytes += active_table->get_size();
    frozen_table.emplace(frozen_table.begin(), std::move(active_table));
    active_table = std::make_shared<SkipList>();
}

size_t MemTable::get_total_size() {
    std::shared_lock<std::shared_mutex> act_lock(active_mutex);
    std::shared_lock<std::shared_mutex> frz_lock(frozen_mutex);
    return active_table->get_size() + frozen_bytes;
}

size_t MemTable::get_active_size() {
    std::shared_lock<std::shared_mutex> act_lock(active_mutex);
    return active_table->get_size();
}

size_t MemTable::get_frozen_size() {
    std::shared_lock<std::shared_mutex> act_lock(frozen_mutex);
    return frozen_bytes;
}
}  // LOG STRUCT MERGE TREE
