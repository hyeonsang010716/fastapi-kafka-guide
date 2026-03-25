#!/usr/bin/env bash
set -euo pipefail

PASS="\033[32m[PASS]\033[0m"
FAIL="\033[31m[FAIL]\033[0m"

echo "========================================"
echo "  개발 환경 점검 스크립트"
echo "========================================"
echo ""

# --- Docker ---
if command -v docker &> /dev/null; then
    docker_version=$(docker --version)
    echo -e "$PASS Docker 설치됨 - $docker_version"
else
    echo -e "$FAIL Docker가 설치되어 있지 않습니다. https://www.docker.com/products/docker-desktop/ 에서 설치하세요."
fi

# --- Docker Compose ---
if docker compose version &> /dev/null; then
    compose_version=$(docker compose version)
    echo -e "$PASS Docker Compose 설치됨 - $compose_version"
else
    echo -e "$FAIL Docker Compose가 설치되어 있지 않습니다. Docker Desktop을 최신 버전으로 업데이트하세요."
fi

# --- Python 3.12+ ---
python_cmd=""
for cmd in python3 python; do
    if command -v "$cmd" &> /dev/null; then
        python_cmd="$cmd"
        break
    fi
done

if [[ -n "$python_cmd" ]]; then
    python_version=$($python_cmd --version 2>&1 | awk '{print $2}')
    major=$(echo "$python_version" | cut -d. -f1)
    minor=$(echo "$python_version" | cut -d. -f2)

    if [[ "$major" -ge 3 && "$minor" -ge 12 ]]; then
        echo -e "$PASS Python 설치됨 - Python $python_version (>= 3.12)"
    else
        echo -e "$FAIL Python $python_version 이 설치되어 있지만 3.12 이상이 필요합니다."
    fi
else
    echo -e "$FAIL Python이 설치되어 있지 않습니다. https://www.python.org/downloads/ 에서 설치하세요."
fi

echo ""
echo "========================================"
echo "  점검 완료"
echo "========================================"
