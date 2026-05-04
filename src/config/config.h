#pragma once

#include <string>
#include <vector>

namespace LSMT {
class TomlConfig {
public:
    ~TomlConfig();

    long long get_lsm_sum_memtable_size() const;

    long long get_lsm_per_memtable_size() const;

    int get_lsm_sst_level_ratio() const;

    int get_lsm_block_size() const;

    int get_lsm_block_cache_size() const;

    int get_lsm_block_cache_lruk() const;

    int get_bloom_filter_expected_elements() const;

    double get_bloom_filter_false_positive_rate() const;

    static const TomlConfig &get_instance(const std::string &file_path = "config.toml");

private:
    TomlConfig(const std::string &file_path);

    bool load_config_file();

    bool save_config_file();

    void set_default_value();

private:
    std::string config_file_path;

    long long lsm_sum_memtable_size;
    long long lsm_per_memtable_size;
    int lsm_sst_level_ratio;
    int lsm_block_size;
    int lsm_block_cache_size;
    int lsm_block_cache_lruk;

    int bloom_filter_expected_elements;
    double bloom_filter_false_positive_rate;
};
} // LOG STRUCTURED MERGE TREE