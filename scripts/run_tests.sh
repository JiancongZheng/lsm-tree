#!/usr/bin/env bash
# =========================
# 路径设置
# =========================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$SOURCE_DIR/build/bin"

# =========================
# 参数解析
# =========================
if [ $# -ne 1 ]; then
    echo "Usage: $0 -<module> (e.g., -memtable, -all)"
    exit 1
fi

arg="$1"
if [[ "$arg" != -* ]]; then
    echo "Argument must start with '-'"
    exit 1
fi

TEST="${arg:1}"

if [ "$TEST" = "all" ]; then
    # 运行所有可执行文件
    for file in "$BIN_DIR"/*; do
        if [ -x "$file" ] && [ -f "$file" ]; then
            test_name=$(basename "$file")
            echo "================================="
            "$file"
            echo "================================="
        fi
    done
else
    test_name="test_$TEST"
    if [ -x "$BIN_DIR/$test_name" ]; then
        echo "================================="
        "$BIN_DIR/$test_name"
        echo "================================="
    else
        echo "Test $test_name not found in $BIN_DIR"
        exit 1
    fi
fi