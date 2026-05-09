#!/usr/bin/env bash
set -euo pipefail

MCPERF_BIN="${1:-$HOME/memcache-perf-dynamic/mcperf}"
REPO="$HOME/memcache-perf-dynamic"

if [[ -x "$MCPERF_BIN" ]]; then
  echo "mcperf already installed at $MCPERF_BIN"
  exit 0
fi

sudo sed -i 's/^Types: deb$/Types: deb deb-src/' /etc/apt/sources.list.d/ubuntu.sources || true
sudo apt-get update
sudo apt-get install libevent-dev libzmq3-dev git make g++ --yes
sudo apt-get build-dep memcached --yes

if [[ -d "$REPO/.git" ]]; then
  git -C "$REPO" pull --ff-only
else
  git clone https://github.com/eth-easl/memcache-perf-dynamic.git "$REPO"
fi

make -C "$REPO"
test -x "$MCPERF_BIN"
