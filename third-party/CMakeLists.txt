cmake_minimum_required(VERSION 3.14)

include(FetchContent)

# 设置 FetchContent 参数（可选但推荐）
set(FETCHCONTENT_QUIET FALSE)      # 显示下载进度
set(FETCHCONTENT_UPDATES_DISCONNECTED ON)  # 避免每次构建都检查更新

# 下载安装googletest
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0           # 使用最新稳定版
    GIT_SHALLOW    TRUE              # 只下载最新提交
    GIT_PROGRESS   TRUE              # 显示下载进度
)
FetchContent_MakeAvailable(googletest)

# 下载安装tomlplusplus
FetchContent_Declare(
    tomlplusplus
    GIT_REPOSITORY https://github.com/marzer/tomlplusplus.git
    GIT_TAG        v3.4.0            # 使用最新稳定版
    GIT_SHALLOW    TRUE              # 只下载最新提交
    GIT_PROGRESS   TRUE              # 显示下载进度
)
FetchContent_MakeAvailable(tomlplusplus)
