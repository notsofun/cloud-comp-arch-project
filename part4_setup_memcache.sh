#!/usr/bin/env bash
set -euo pipefail

MEMCACHED_IP="${1:?usage: part4_setup_memcache.sh INTERNAL_MEMCACHED_IP}"

sudo apt-get update
sudo apt-get install -y memcached libmemcached-tools docker.io
sudo systemctl enable --now docker

sudo python3 - "$MEMCACHED_IP" <<'PY'
import sys
from pathlib import Path

path = Path("/etc/memcached.conf")
bind_ip = sys.argv[1]
lines = path.read_text(encoding="utf-8").splitlines()
seen_memory = False
seen_listen = False
seen_threads = False
updated = []

for line in lines:
    if line.startswith("-m"):
        updated.append("-m 1024")
        seen_memory = True
    elif line.startswith("-l"):
        updated.append(f"-l {bind_ip}")
        seen_listen = True
    elif line.startswith("-t"):
        updated.append("-t 4")
        seen_threads = True
    else:
        updated.append(line)

if not seen_memory:
    updated.append("-m 1024")
if not seen_listen:
    updated.append(f"-l {bind_ip}")
if not seen_threads:
    updated.append("-t 4")

path.write_text("\n".join(updated) + "\n", encoding="utf-8")
PY

sudo systemctl restart memcached
sudo systemctl --no-pager --full status memcached | sed -n '1,8p'

for image in \
  anakli/cca:parsec_streamcluster \
  anakli/cca:parsec_freqmine \
  anakli/cca:parsec_canneal \
  anakli/cca:parsec_vips \
  anakli/cca:splash2x_barnes \
  anakli/cca:parsec_blackscholes \
  anakli/cca:splash2x_radix
do
  sudo docker pull "$image"
done
