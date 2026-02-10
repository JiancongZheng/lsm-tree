#include "./config.h"

#include <iostream>
#include <fstream>
#include <toml++/toml.hpp>

namespace LSMT {
TomlConfig::TomlConfig(const std::string &file_path) : config_file_path(file_path) {
    // 不存在配置文件路径或加载配置失败 加载默认配置
    if (config_file_path.empty() || !load_config_file()) {
        set_default_value();
    }
}

TomlConfig::~TomlConfig() {
    // 存在配置文件路径但配置文件不存在 保存当前配置
    if (!config_file_path.empty() && !std::ifstream(config_file_path).good()) {
        save_config_file();
    }
}

bool TomlConfig::load_config_file() {
    try {
        auto config = toml::parse(config_file_path);

        auto lsmt_config = config["lsmt"];
        lsm_sum_memtable_size = lsmt_config.at_path("LSM_SUM_MEMTABLE_SIZE").value<uint64_t>().value();
        lsm_per_memtable_size = lsmt_config.at_path("LSM_PER_MEMTABLE_PATH").value<uint64_t>().value();
        lsm_sst_level_ratio   = lsmt_config.at_path("LSM_SST_LEVEL_RATIO").value<int>().value();
        lsm_block_size        = lsmt_config.at_path("LSM_BLOCK_SIZE").value<int>().value();
        lsm_block_cache_size  = lsmt_config.at_path("LSM_BLOCK_CACHE_SIZE").value<int>().value();
        lsm_block_cache_lruk  = lsmt_config.at_path("LSM_BLOCK_CACHE_LRUK").value<int>().value();

        return true;
    } catch (const std::exception &err) {
        std::cerr << "Error in Load Config File " << config_file_path << ": " << err.what() << std::endl;
        return false;
    }
}

bool TomlConfig::save_config_file() {
    try {
        toml::table config {
            {"lsmt", toml::table {
                {"LSM_SUM_MEMTABLE_SIZE", 67108864},
                {"LSM_PER_MEMTABLE_SZIE", 4194304},
                {"LSM_SST_LEVEL_RATIO",   4},
                {"LSM_BLOCK_SIZE",        32768},
                {"LSM_BLOCK_CACHE_SIZE",  1024},
                {"LSM_BLOCK_CACHE_LRUK",  8},
            }},
            {"redis", toml::table{

            }},
            {"bloom_filter", toml::table{

            }},
        };

        std::ofstream file(config_file_path, std::ios::out | std::ios::trunc);
        if (file.is_open()) {
            return (file << config).good();
        }
    } catch (const std::exception& err) {
        std::cerr << "Error in Save Config File " << config_file_path << ": " << err.what() << std::endl;
        return false;
    };
}

void TomlConfig::set_default_value() {
    lsm_sum_memtable_size = 1024 * 1024 * 64;
    lsm_per_memtable_size = 1024 * 1024 * 4;
    lsm_sst_level_ratio   = 4;
    lsm_block_size        = 1024 * 32;
    lsm_block_cache_size  = 1024;
    lsm_block_cache_lruk  = 8;
}

const TomlConfig &TomlConfig::getInstance(const std::string &file_path = "config.toml") {
    static const TomlConfig instance([](const std::string &path) -> std::string {
        std::ifstream file(path);
        return file.good() ? path : std::string("config.toml");
    }(file_path));
    return instance;
}


long long TomlConfig::get_lsm_sum_memtable_size() const {
    return lsm_sum_memtable_size;
}

long long TomlConfig::get_lsm_per_memtable_size() const {
    return lsm_per_memtable_size;
}

int TomlConfig::get_lsm_sst_level_ratio() const {
    return lsm_sst_level_ratio;
}

int TomlConfig::get_lsm_block_size() const {
    return lsm_block_size;
}

int TomlConfig::get_lsm_block_cache_size() const {
    return lsm_block_cache_size;
}

int TomlConfig::get_lsm_block_cache_lruk() const {
    return lsm_block_cache_lruk;
}
} // LOG STRUCTURED MERGE TREE