# LSM Tree

一个用于学习和实践 LSM Tree 存储引擎设计的 C++ 项目，参考了香草美人的 Tiny-LSM：
[Vanilla-Beauty/tiny-lsm](https://github.com/Vanilla-Beauty/tiny-lsm)

## 当前实现

目前代码中已经包含以下模块：

- `skiplist`：有序内存结构，负责基础写入、查找、删除与遍历
- `memtable`：基于 SkipList 的内存表封装
- `block`：SST 中的数据块、元信息与块迭代器
- `sst`：SST 文件读写、索引定位、Bloom Filter 检查与迭代访问
- `config`：基于 `toml++` 的配置文件读取
- `utils`：文件操作、Bloom Filter、游标等通用工具
- `lsm`：LSM 引擎接口，包含 `put/get/remove/flush` 等能力

## 开发环境

建议环境：

- C++17 及以上编译器
- CMake 3.14 及以上
- `make` 或 `ninja`
- 可访问网络的 Git 环境

项目当前通过 `FetchContent` 自动拉取第三方依赖：

- `googletest`
- `tomlplusplus`

首次配置构建目录时，`CMake` 会自动下载这些依赖。

## 配置说明

项目通过环境变量 `LSMT_CONFIG` 指定配置文件路径。

```bash
export LSMT_CONFIG=/绝对路径/lsm-tree/config.toml
```

### 使用构建脚本

```bash
sh scripts/build.sh -ninja -release
```

可选参数：

- `-ninja` 或 `ninja`：使用 Ninja 生成器
- `-make` 或 `make`：使用 Unix Makefiles
- `-debug` 或 `debug`：构建 Debug 版本
- `-release` 或 `release`：构建 Release 版本

### 手动构建

如果你更习惯直接使用 `cmake`：

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```