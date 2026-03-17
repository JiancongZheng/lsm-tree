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
if [[ "$1" == "ninja" ]]; then
    GENERATOR="Ninja"
elif [[ "$1" == "make" ]]; then
    GENERATOR="Unix Makefiles"
else
    if command -v ninja >/dev/null 2>&1; then
        GENERATOR="Ninja"
    else
        GENERATOR="Unix Makefiles"
    fi
fi

# =========================
# 构建编译
# =========================
mkdir -p "$BUILD_DIR"

beg_build=$(date +%s%3N)
cmake -S "$SOURCE_DIR" -B "$BUILD_DIR" -G "$GENERATOR" || exit 1
end_build=$(date +%s%3N)
btime=$((end_build-beg_build))

beg_compile=$(date +%s%3N)
cmake --build "$BUILD_DIR" -j || exit 1
end_compile=$(date +%s%3N)
ctime=$((end_compile-beg_compile))

echo "================================="
echo "项目编译工具: ${GENERATOR}"
echo "项目构建时间: ${btime} ms"
echo "项目编译时间: ${ctime} ms"
echo "项目消耗时间: $((btime+ctime)) ms"
echo "================================="