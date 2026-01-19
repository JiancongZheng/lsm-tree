#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
THIRD_PARTY_DIR="${ROOT_DIR}/third-party"

mkdir -p "${THIRD_PARTY_DIR}"
cd "${THIRD_PARTY_DIR}"

clone_gtest() {
    local name="googletest"
    local repo="https://github.com/google/googletest.git"
    local branch="v1.14.0"

    if [ ! -d "${name}" ]; then
        echo "==> Cloning third-party: ${name} waiting..."
        git clone --depth=1 -b "${branch}" "${repo}" "${name}" || exit 1
        echo "==> Cloning third-party: ${name} succeed!!!"
    else
        echo "==> Third-party ${name} already exists, skip!"
    fi
}

clone_gtest