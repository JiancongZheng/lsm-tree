#include <algorithm>
#include <filesystem>
#include <shared_mutex>

#include "lsm_engine.h"

namespace LSMT {
LSMTEngine::LSMTEngine(std::string path) : lsmt_path(path) {
    block_cache = std::make_shared<BlockCache>(
        TomlConfig::get_instance().get_lsm_block_cache_size(),
        TomlConfig::get_instance().get_lsm_block_cache_lruk());
    
    if (std::filesystem::exists(lsmt_path) == false) {
        std::filesystem::create_directory(lsmt_path);
    }

    if (std::filesystem::exists(lsmt_path) == false) {
        throw std::runtime_error("Failed to create LSM tree directory") ;
    }

    for (const auto &entry : std::filesystem::directory_iterator(lsmt_path)) {
        if (!entry.is_regular_file()) {
            continue;
        }

        std::string filename = entry.path().filename().string();
        if (filename.substr(0, 4) != "sst_") {
            continue;
        }

        const size_t dot_pos = filename.find('.');
        if (dot_pos == std::string::npos || dot_pos == filename.length() - 1 || dot_pos <= 4) {
            continue;
        }

        const std::string index_str = filename.substr(4, dot_pos - 4);
        const std::string level_str = filename.substr(dot_pos + 1, filename.length() - dot_pos - 1);
        if (index_str.empty() || level_str.empty()) {
            continue;
        }
        size_t sst_index = std::stoull(index_str);
        size_t sst_level = std::stoull(level_str);

        std::unique_lock<std::shared_mutex> lsmt_lock(lsmt_mutex);

        auto sst = SST::open(sst_index, FileObj::open(entry.path().string(), false), block_cache);
        ssts[sst_index] = sst;
        sst_indexes[sst_level].push_back(sst_index);
        curr_max_level = std::max(curr_max_level, sst_level);
        next_sst_index = std::max(next_sst_index, sst_index + 1);
    }

    for (auto &[level, sst_id_list] : sst_indexes) {
        std::sort(sst_id_list.begin(), sst_id_list.end());
        if (level == 0) {
            std::reverse(sst_id_list.begin(), sst_id_list.end());
        }
    }
}

std::optional<std::pair<std::string, uint64_t>> LSMTEngine::get(const std::string &key, uint64_t trx_id) {
    // 在MemTable中查找目标键值对
    auto mem_result = memtable.get(key, trx_id);
    if (mem_result.is_vld()) {
        return std::pair<std::string, uint64_t>(mem_result.get_val(), mem_result.get_trx_id());
    }

    std::shared_lock<std::shared_mutex> rd_lock(lsmt_mutex);
    // 在无序的Level0中所有SSTable中查找目标键值对
    for (auto &sst_index : sst_indexes[0]) {
        auto sst = ssts[sst_index];
        auto sst_result = sst->get(key, trx_id);
        if (sst_result.is_vld()) {
            return std::pair<std::string, uint64_t>(sst_result.get_val(), sst_result.get_trx_id());
        } else {
            continue;  // Level0 SST需要全部遍历查询
        }
    }
    // 在有序的LevelN中目标SSTable中查找目标键值对
    for (size_t level = 1; level <= curr_max_level; ++level) {
        size_t lk = 0;
        size_t rk = sst_indexes[level].size() - 1;
        while (lk <= rk) {
            auto mid = lk + (rk - lk) / 2;
            auto sst = ssts[sst_indexes[level][mid]];
            if (sst->get_fkey() <= key && sst->get_lkey() >= key) {
                auto sst_result = sst->get(key, trx_id);
                if (sst_result.is_vld()) {
                    return std::pair<std::string, uint64_t>(sst_result.get_val(), sst_result.get_trx_id());
                } else {
                    break;  // 当前Level不可能存在目标键值对 跳出循环在下一个Level继续查找
                }
            } else if (sst->get_fkey() > key) {
                rk = mid - 1;
            } else if (sst->get_lkey() < key) {
                lk = mid + 1;
            } else {
                // Nothing to do
            }
        }
    }

    return std::nullopt;
}

std::vector<std::pair<std::string, std::optional<std::pair<std::string, uint64_t>>>>
LSMTEngine::get(const std::vector<std::string> &keys, uint64_t trx_id) {
    // 在MemTable中批量查找目标键值对
    auto results = memtable.get(keys, trx_id);
    if (std::all_of(results.begin(), results.end(), [](const auto &e) { return e.second.has_value(); })) {
        return results;
    }

    std::shared_lock<std::shared_mutex> rd_lock(lsmt_mutex);
    // 在无序的Level0中所有SSTable中批量查找目标键值对
    for (auto &[key, optional_val] : results) {
        if (optional_val.has_value()) {
            continue;
        }
        for (auto &sst_index : sst_indexes[0]) {
            auto sst = ssts[sst_index];
            auto sst_reuslt = sst->get(key, trx_id);
            if (sst_reuslt.is_vld()) {
                optional_val = std::make_pair(sst_reuslt.get_val(), sst_reuslt.get_trx_id());
                break;
            }
        }
    }
    // 在有序的LevelN中目标SSTable中批量查找目标键值对
    for (size_t level = 1; level <= curr_max_level; ++level) {
        for (auto &[key, optional_val] : results) {
            if (optional_val.has_value()) {
                continue;
            }
            size_t lk = 0;
            size_t rk = sst_indexes[level].size() - 1;
            while (lk <= rk) {
                auto mid = lk + (rk - lk) / 2;
                auto sst = ssts[sst_indexes[level][mid]];
                if (sst->get_fkey() <= key && sst->get_lkey() >= key) {
                    auto sst_result = sst->get(key, trx_id);
                    if (sst_result.is_vld()) {
                        optional_val = std::make_pair(sst_result.get_val(), sst_result.get_trx_id());
                    }
                    break;  // 当前Level不可能存在目标键值对 跳出循环在下一个Level继续查找
                } else if (sst->get_fkey() > key) {
                    rk = mid - 1;
                } else if (sst->get_lkey() < key) {
                    lk = mid + 1;
                } else {
                    // Nothing to do
                }
            }
        }
    }

    return results;
}

uint64_t LSMTEngine::put(const std::string &key, const std::string &val, uint64_t trx_id) {
    memtable.put(key, val, trx_id);
    if (memtable.get_total_size() >= TomlConfig::get_instance().get_lsm_sum_memtable_size()) {
        return flush();
    }
    return 0;
}

uint64_t LSMTEngine::put(const std::vector<std::pair<std::string, std::string>> &kv_pairs, uint64_t trx_id) {
    memtable.put(kv_pairs, trx_id);
    if (memtable.get_total_size() >= TomlConfig::get_instance().get_lsm_sum_memtable_size()) {
        return flush();
    }
    return 0;
}

uint64_t LSMTEngine::remove(const std::string &key, uint64_t trx_id) {
    memtable.remove(key, trx_id);
    if (memtable.get_total_size() >= TomlConfig::get_instance().get_lsm_sum_memtable_size()) {
        return flush();
    }
    return 0;
}

uint64_t LSMTEngine::remove(const std::vector<std::string> &keys, uint64_t trx_id) {
    memtable.remove(keys, trx_id);
    if (memtable.get_total_size() >= TomlConfig::get_instance().get_lsm_sum_memtable_size()) {
        return flush();
    }
    return 0;
}

void LSMTEngine::clear() {
    memtable.clear();
    sst_indexes.clear();
    ssts.clear();
    try {
        for (const auto &entry : std::filesystem::directory_iterator(lsmt_path)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            std::filesystem::remove(entry.path());
        }
    } catch (const std::filesystem::filesystem_error &e) {
        throw std::runtime_error("Failed to clear LSMT directory: " + std::string(e.what()));
    }
}

uint64_t LSMTEngine::flush() {
    if (memtable.get_total_size() == 0) {
        return 0;
    }

    std::unique_lock<std::shared_mutex> lsmt_lock(lsmt_mutex);

    if (sst_indexes[0].size() > TomlConfig::get_instance().get_lsm_sst_level_ratio()) {
        compact(0, 1);
    }

    size_t new_sst_id = next_sst_index++;

    SSTBuilder builder = SSTBuilder(TomlConfig::get_instance().get_lsm_block_size(), true);

    std::vector<uint64_t> trx_ids;
    auto sst_path = get_sst_path(new_sst_id, 0);
    auto new_sst = memtable.flush(builder, sst_path, new_sst_id, trx_ids, block_cache);

    ssts[new_sst_id] = new_sst;
    sst_indexes[0].push_front(new_sst_id);

    return new_sst->get_trx_id_range().second;
}

void LSMTEngine::compact(size_t src_level, size_t dst_level) {
    // 递归检查src_level + 1层是否需要执行合并操作
    if (sst_indexes[src_level + 1].size() >= TomlConfig::get_instance().get_lsm_sst_level_ratio()) {
        compact(src_level + 1, dst_level + 1);
    }

    std::vector<std::shared_ptr<SST>> new_ssts;
    auto src_indexes = sst_indexes[src_level];
    auto dst_indexes = sst_indexes[dst_level];
    std::vector<size_t> src_indexes_vec(src_indexes.begin(), src_indexes.end());
    std::vector<size_t> dst_indexes_vec(dst_indexes.begin(), dst_indexes.end());

    // 对Level0执行全量合并 对LevelN执行分区合并
    if (src_level == 0) {
        new_ssts = full_compact(src_indexes_vec, dst_indexes_vec, dst_level);
    } else {
        new_ssts = zone_compact(src_indexes_vec, dst_indexes_vec, dst_level);
    }

    // 删除旧SSTable文件和内存中SSTable的索引信息
    for (auto &src_index : src_indexes) {
        ssts[src_index]->remove();
        ssts.erase(src_index);
    }
    for (auto &dst_index : dst_indexes) {
        ssts[dst_index]->remove();
        ssts.erase(dst_index);
    }
    sst_indexes[src_level].clear();
    sst_indexes[dst_level].clear();

    curr_max_level = std::max(curr_max_level, dst_level);

    // 将新生成的SSTable添加到内存中SSTable的索引信息
    for (auto &new_sst : new_ssts) {
        sst_indexes[dst_level].push_back(new_sst->get_sst_id());
        ssts[new_sst->get_sst_id()] = new_sst;
    }
    std::sort(sst_indexes[dst_level].begin(), sst_indexes[dst_level].end());
}

std::vector<std::shared_ptr<SST>> LSMTEngine::full_compact(std::vector<size_t> &src_indexes, 
        std::vector<size_t> &dst_indexes, size_t dst_level) {
    // 获取src_level中所有SSTable并合并为HeapIterator
    std::vector<SSTIterator> src_iters;
    src_iters.reserve(src_indexes.size());
    for (auto &src_index : src_indexes) {
        src_iters.push_back(ssts[src_index]->begin(0));
    }
    auto src_heap_pair = SSTIterator::merge_sst_iterator(std::move(src_iters), 0);
    auto src_iter_ptr = std::make_shared<HeapIterator>(std::move(src_heap_pair.first));

    // 获取dst_level中所有SSTable并构造为ConcatIterator
    std::vector<std::shared_ptr<SST>> dst_ssts;
    for (auto &dst_index : dst_indexes) {
        dst_ssts.push_back(ssts[dst_index]);
    }
    auto dst_iter_ptr = std::make_shared<ConcatIterator>(std::move(dst_ssts), 0);
    // 对src_level和dst_level的SSTable执行合并操作并返回新生成的SSTable
    TwoMergeIterator merge_iter(src_iter_ptr, dst_iter_ptr, 0);
    return generate_ssts(merge_iter, get_sst_size(dst_level), dst_level);
}

std::vector<std::shared_ptr<SST>> LSMTEngine::zone_compact(std::vector<size_t> &src_indexes,
        std::vector<size_t> &dst_indexes, size_t dst_level) {
    // 获取src_level中所有SSTable并构造为ConcatIterator
    std::vector<std::shared_ptr<SST>> src_ssts;
    for (auto &src_index : src_indexes) {
        src_ssts.push_back(ssts[src_index]);
    }
    auto src_iter_ptr = std::make_shared<ConcatIterator>(std::move(src_ssts), 0);
    // 获取dst_level中所有SSTable并构造为ConcatIterator
    std::vector<std::shared_ptr<SST>> dst_ssts;
    for (auto &dst_index : dst_indexes) {
        dst_ssts.push_back(ssts[dst_index]);
    }
    auto dst_iter_ptr = std::make_shared<ConcatIterator>(std::move(dst_ssts), 0);
    // 对src_level和dst_level的SSTable执行合并操作并返回新生成的SSTable
    TwoMergeIterator merge_iter(src_iter_ptr, dst_iter_ptr, 0);
    return generate_ssts(merge_iter, get_sst_size(dst_level), dst_level);
}

std::vector<std::shared_ptr<SST>> LSMTEngine::generate_ssts(BaseIterator &iter, size_t size, size_t level) {
    std::vector<std::shared_ptr<SST>> new_ssts;
    auto builder = SSTBuilder(TomlConfig::get_instance().get_lsm_block_size(), true);
    
    while (iter.is_vld() && !iter.is_end()) {
        std::string curr_key = (*iter).first;
        builder.add((*iter).first, (*iter).second, iter.get_trx_id());
        ++iter;
        if (!(iter.is_vld() && !iter.is_end() && (*iter).first == curr_key) &&
                builder.estimated_size() >= size) {
            size_t new_sst_index = next_sst_index++;
            std::string sst_path = get_sst_path(new_sst_index, level);
            auto new_sst = builder.build(new_sst_index, sst_path, block_cache);
            new_ssts.push_back(new_sst);
            builder = SSTBuilder(TomlConfig::get_instance().get_lsm_block_size(), true);
        }
    }

    if (builder.real_size() > 0) {
        size_t new_sst_index = next_sst_index++;
        std::string sst_path = get_sst_path(new_sst_index, level);
        auto new_sst = builder.build(new_sst_index, sst_path, block_cache);
        new_ssts.push_back(new_sst);
    }

    return new_ssts;
}

std::string LSMTEngine::get_sst_path(size_t sst_index, size_t sst_level) {
    // 文件路径格式 lsmt_path/sst_<sst_index>.<sst_level>
    std::stringstream ss;
    ss << lsmt_path << "/sst_" << std::setfill('0') << std::setw(32) << sst_index << "." << sst_level;
    return ss.str();
}

size_t LSMTEngine::get_sst_size(size_t level) {
    return TomlConfig::get_instance().get_lsm_per_memtable_size() *
        static_cast<size_t>(std::pow(TomlConfig::get_instance().get_lsm_sst_level_ratio(), level));
}

LevelIterator LSMTEngine::begin(uint64_t trx_id) {
    return LevelIterator(shared_from_this(), trx_id);
}

LevelIterator LSMTEngine::end() {
    return LevelIterator();
}

std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
LSMTEngine::iter_monotony_predicate(uint64_t trx_id, std::function<int(const std::string&)> predicate) {
    // 获取MemTable中满足单调性谓词的迭代器
    auto memtable_result = memtable.iters_monotony_predicate(trx_id, predicate);

    // 获取SSTable中满足单调性谓词的迭代器
    std::vector<Item> items;
    for (auto &[level, indexes] : sst_indexes) {
        for (auto &index : indexes) {
            auto sst_result = ssts[index]->iters_monotony_predicate(trx_id, predicate);
            if (!sst_result.has_value()) {
                continue;
            }
            auto [begin, end] = sst_result.value();
            for (; begin != end && begin.is_vld(); ++begin) {
                if ((trx_id != 0 && begin.get_trx_id() > trx_id) ||
                    (items.empty() == false && items.back().key == (*begin).first)) {
                    continue;
                }
                items.emplace_back((*begin).first, (*begin).second, -index, level, begin.get_trx_id());
            }
        }
    }
    // 对MemTable和SSTable中满足单调性谓词的迭代器执行合并操作并返回结果
    if (!memtable_result.has_value() && items.empty()) {
        return std::nullopt;
    }
    if (memtable_result.has_value()) {
        auto mem_iter_ptr = std::make_shared<HeapIterator>(memtable_result.value().first);
        auto sst_iter_ptr = std::make_shared<HeapIterator>(items, trx_id);
        return std::make_optional<std::pair<TwoMergeIterator, TwoMergeIterator>>(
            TwoMergeIterator(mem_iter_ptr, sst_iter_ptr, trx_id), TwoMergeIterator()
        );
    } else {
        auto mem_iter_ptr = std::make_shared<HeapIterator>();
        auto sst_iter_ptr = std::make_shared<HeapIterator>(items, trx_id);
        return std::make_optional<std::pair<TwoMergeIterator, TwoMergeIterator>>(
            TwoMergeIterator(mem_iter_ptr, sst_iter_ptr, trx_id), TwoMergeIterator()
        );
    }
}




LSMTree::LSMTree(std::string lsmt_path) : engine(std::make_shared<LSMTEngine>(lsmt_path)) { }

LSMTree::~LSMTree() { flush_all(); }

std::optional<std::string> LSMTree::get(const std::string &key) {
    auto result = engine->get(key, 0);
    if (result.has_value() && !result.value().first.empty()) {
        return std::make_optional(result.value().first);
    } else {
        return std::nullopt;
    }
}

std::vector<std::pair<std::string, std::optional<std::string>>> 
LSMTree::get(const std::vector<std::string> &keys) {
    std::vector<std::pair<std::string, std::optional<std::string>>> final_result;
    
    auto result = engine->get(keys, 0);
    for (const auto &[key, value] : result) {
        if (value.has_value() && !value.value().first.empty()) {
            final_result.emplace_back(key, value.value().first);
        } else {
            final_result.emplace_back(key, std::nullopt);
        }
    }
    return final_result;
}

void LSMTree::put(const std::string &key, const std::string &val) {
    engine->put(key, val, 0);
}

void LSMTree::put(const std::vector<std::pair<std::string, std::string>> &kv_pairs) {
    engine->put(kv_pairs, 0);
}

void LSMTree::remove(const std::string &key) {
    engine->remove(key, 0);
}

void LSMTree::remove(const std::vector<std::string> &keys) {
    engine->remove(keys, 0);
}

LevelIterator LSMTree::begin(uint64_t trx_id) {
    return engine->begin(trx_id);
}

LevelIterator LSMTree::end() {
    return engine->end();
}

std::optional<std::pair<TwoMergeIterator, TwoMergeIterator>>
LSMTree::iter_monotony_predicate(uint64_t trx_id, std::function<int(const std::string&)> predicate) {
    return engine->iter_monotony_predicate(trx_id, predicate);
}

void LSMTree::clear() {
    engine->clear();
}

void LSMTree::flush_one() {
    auto max_trx_id = engine->flush();
}

void LSMTree::flush_all() {
    while (engine->memtable.get_total_size() > 0) {
        auto max_trx_id = engine->flush();
    }
}

} // LOG STRUCTURED MERGE TREE
