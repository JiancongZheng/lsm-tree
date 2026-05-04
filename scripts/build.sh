#!/usr/bin/env bash
# =========================
# 路径设置
# =========================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$SOURCE_DIR/build"
# =========================
# 参数解析
# =========================
GENERATOR="Unix Makefiles"
BUILD_TYPE="Release"

for arg in "$@"; do
    case "$arg" in
        ninja|-ninja)
            GENERATOR="Ninja"
            ;;
        make|-make)
            GENERATOR="Unix Makefiles"
            ;;
        debug|-debug)
            BUILD_TYPE="Debug"
            ;;
        release|-release)
            BUILD_TYPE="Release"
            ;;
        *)
            echo "Unknown argument: $arg"
            echo "Usage: $0 [-ninja|-make] [-debug|-release]"
            exit 1
            ;;
    esac
done

# =========================
# 构建编译
# =========================
mkdir -p "$BUILD_DIR"

beg_build=$(date +%s%3N)
cmake -S "$SOURCE_DIR" -B "$BUILD_DIR" -G "$GENERATOR" -DCMAKE_BUILD_TYPE="$BUILD_TYPE" || exit 1
end_build=$(date +%s%3N)
btime=$((end_build-beg_build))

beg_compile=$(date +%s%3N)
cmake --build "$BUILD_DIR" -j || exit 1
end_compile=$(date +%s%3N)
ctime=$((end_compile-beg_compile))

echo "================================="
echo "项目编译工具: ${GENERATOR}"
echo "项目构建类型: ${BUILD_TYPE}"
echo "项目构建时间: ${btime} ms"
echo "项目编译时间: ${ctime} ms"
echo "项目消耗时间: $((btime+ctime)) ms"
echo "================================="
