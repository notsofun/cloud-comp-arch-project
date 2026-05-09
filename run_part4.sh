#!/usr/bin/env bash
set -euo pipefail

GROUP="${GROUP:-065}"
RUNS="${RUNS:-3}"
POLICY_SET="${POLICY_SET:-both}"
DURATION="${DURATION:-1800}"
QPS_INTERVAL="${QPS_INTERVAL:-15}"
QPS_MIN="${QPS_MIN:-5000}"
QPS_MAX="${QPS_MAX:-110000}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-5}"
CONTROLLER_GRACE="${CONTROLLER_GRACE:-900}"
SETUP="${SETUP:-1}"
CREATE_CLUSTER="${CREATE_CLUSTER:-0}"
DELETE_CLUSTER="${DELETE_CLUSTER:-0}"
GENERATE_PLOTS="${GENERATE_PLOTS:-1}"

export KOPS_STATE_STORE="${KOPS_STATE_STORE:-gs://cca-eth-2026-group-6065-ethzid}"

if [[ "$CREATE_CLUSTER" == "1" ]]; then
  if ! kops get cluster part4.k8s.local >/dev/null 2>&1; then
    kops create -f part4.yaml
  fi
  kops update cluster --name part4.k8s.local --yes --admin
  kops validate cluster --wait 10m
fi

if [[ "$SETUP" == "1" ]]; then
  python3 part4_runner.py \
    --group "$GROUP" \
    --runs "$RUNS" \
    --policy-set "$POLICY_SET" \
    --duration "$DURATION" \
    --qps-interval "$QPS_INTERVAL" \
    --qps-min "$QPS_MIN" \
    --qps-max "$QPS_MAX" \
    --sample-interval "$SAMPLE_INTERVAL" \
    --controller-grace "$CONTROLLER_GRACE" \
    --setup
else
  python3 part4_runner.py \
    --group "$GROUP" \
    --runs "$RUNS" \
    --policy-set "$POLICY_SET" \
    --duration "$DURATION" \
    --qps-interval "$QPS_INTERVAL" \
    --qps-min "$QPS_MIN" \
    --qps-max "$QPS_MAX" \
    --sample-interval "$SAMPLE_INTERVAL" \
    --controller-grace "$CONTROLLER_GRACE"
fi

if [[ "$GENERATE_PLOTS" == "1" ]]; then
  python3 part4_generate_plots.py --group "$GROUP"
fi

if [[ "$DELETE_CLUSTER" == "1" ]]; then
  kops delete cluster --name part4.k8s.local --yes
fi
