#!/bin/bash

set -e

BASE_DIR=$(pwd)
LOG_DIR="$BASE_DIR/logs_part2b"
mkdir -p "$LOG_DIR"

benchmarks=(
  parsec-barnes.yaml
  parsec-blackscholes.yaml
  parsec-canneal.yaml
  parsec-freqmine.yaml
  parsec-radix.yaml
  parsec-streamcluster.yaml
  parsec-vips.yaml
)

threads=(1 2 4 8)

for b in "${benchmarks[@]}"; do

  echo "=============================="
  echo "Benchmark: $b"
  echo "=============================="

  for t in "${threads[@]}"; do

    echo "---- Running $b with $t threads ----"

    # 清理
    kubectl delete jobs --all --ignore-not-found
    kubectl delete pods --all --ignore-not-found

    sleep 5

    # 修改 -n 参数（临时替换）
    sed "s/-n [0-9]\+/-n $t/" \
      "parsec-benchmarks/part2b/$b" > /tmp/tmp.yaml

    # 运行 benchmark
    kubectl apply -f /tmp/tmp.yaml

    # 等待 job 完成
    JOB=$(kubectl get jobs -o jsonpath='{.items[0].metadata.name}')
    kubectl wait --for=condition=complete job/$JOB --timeout=2h

    # log
    POD=$(kubectl get pods --selector=job-name=$JOB \
      -o jsonpath='{.items[0].metadata.name}')

    kubectl logs $POD > "$LOG_DIR/${b%.yaml}_t${t}.log"

    echo "Saved: ${b}_t${t}"

  done

done

echo "ALL DONE"