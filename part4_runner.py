#!/usr/bin/env python3
"""
Local orchestration runner for CCA Part 4.

The runner discovers the Part 4 kops nodes, prepares the remote machines,
starts mcperf, starts the controller on memcache-server, and collects the two
required result files for each run.
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent
ZONE = os.environ.get("GCP_ZONE", "europe-west1-b")
SSH_KEY = os.path.expanduser(os.environ.get("CCA_SSH_KEY", "~/.ssh/cloud-computing"))
MCPERF_BIN = os.environ.get("MCPERF_BIN", "~/memcache-perf-dynamic/mcperf")


def run(cmd: list[str], *, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(
        cmd,
        cwd=ROOT,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )


def kubectl(*args: str, capture: bool = False, check: bool = True) -> subprocess.CompletedProcess:
    return run(["kubectl", *args], check=check, capture=capture)


def node_name(label_value: str) -> str:
    out = kubectl(
        "get",
        "nodes",
        "-l",
        f"cca-project-nodetype={label_value}",
        "-o",
        "jsonpath={.items[0].metadata.name}",
        capture=True,
    ).stdout.strip()
    if not out:
        raise RuntimeError(f"no node found for cca-project-nodetype={label_value}")
    return out


def node_internal_ip(label_value: str) -> str:
    out = kubectl(
        "get",
        "nodes",
        "-l",
        f"cca-project-nodetype={label_value}",
        "-o",
        "jsonpath={.items[0].status.addresses[?(@.type=='InternalIP')].address}",
        capture=True,
    ).stdout.strip()
    if not out:
        raise RuntimeError(f"no internal IP found for {label_value}")
    return out


def node_external_ip_by_name(node: str) -> str:
    out = kubectl(
        "get",
        "node",
        node,
        "-o",
        "jsonpath={.status.addresses[?(@.type=='ExternalIP')].address}",
        capture=True,
    ).stdout.strip()
    if not out:
        raise RuntimeError(f"no external IP found for node {node}")
    return out


def _ssh_cmd(node: str, command: str) -> list[str]:
    ext_ip = node_external_ip_by_name(node)
    return [
        "ssh",
        "-i",
        SSH_KEY,
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "ConnectTimeout=30",
        "-o",
        "BatchMode=yes",
        "-o",
        "LogLevel=ERROR",
        f"ubuntu@{ext_ip}",
        command,
    ]


def ssh(node: str, command: str, *, capture: bool = False, retries: int = 4) -> subprocess.CompletedProcess:
    last_exc: subprocess.CalledProcessError | None = None
    for attempt in range(1, retries + 1):
        try:
            return run(_ssh_cmd(node, command), capture=capture)
        except subprocess.CalledProcessError as exc:
            last_exc = exc
            if attempt < retries:
                wait = 10 * attempt
                print(f"[ssh] attempt {attempt}/{retries} to {node} failed, retrying in {wait}s", flush=True)
                time.sleep(wait)
    assert last_exc is not None
    raise last_exc


def ssh_popen(node: str, command: str, out_file: Path) -> subprocess.Popen:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out = out_file.open("w", encoding="utf-8")
    print("+", " ".join(_ssh_cmd(node, command)), flush=True)
    return subprocess.Popen(
        _ssh_cmd(node, command),
        cwd=ROOT,
        stdout=out,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _scp_base(node: str) -> tuple[str, list[str]]:
    ext_ip = node_external_ip_by_name(node)
    return ext_ip, [
        "scp",
        "-i",
        SSH_KEY,
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "LogLevel=ERROR",
    ]


def scp_to(node: str, local: Path, remote: str) -> None:
    ext_ip, base = _scp_base(node)
    run([*base, str(local), f"ubuntu@{ext_ip}:{remote}"])


def scp_from(node: str, remote: str, local: Path) -> None:
    local.parent.mkdir(parents=True, exist_ok=True)
    ext_ip, base = _scp_base(node)
    run([*base, f"ubuntu@{ext_ip}:{remote}", str(local)])


def upload_support_files(mem_node: str, agent_node: str, measure_node: str) -> None:
    for local, remote in [
        (ROOT / "part4_controller.py", "/tmp/cca_part4_controller.py"),
        (ROOT / "scheduler_logger.py", "/tmp/scheduler_logger.py"),
        (ROOT / "part4_setup_memcache.sh", "/tmp/cca_part4_setup_memcache.sh"),
    ]:
        scp_to(mem_node, local, remote)
    ssh(mem_node, "chmod +x /tmp/cca_part4_controller.py /tmp/cca_part4_setup_memcache.sh")

    for node in (agent_node, measure_node):
        scp_to(node, ROOT / "part4_setup_mcperf.sh", "/tmp/cca_part4_setup_mcperf.sh")
        ssh(node, "chmod +x /tmp/cca_part4_setup_mcperf.sh")


def setup_nodes(mem_node: str, agent_node: str, measure_node: str, memcached_ip: str) -> None:
    upload_support_files(mem_node, agent_node, measure_node)
    ssh(mem_node, f"/tmp/cca_part4_setup_memcache.sh {shlex.quote(memcached_ip)}", retries=2)

    setup_arg = "" if MCPERF_BIN == "~/memcache-perf-dynamic/mcperf" else f" {shlex.quote(MCPERF_BIN)}"
    for node in (agent_node, measure_node):
        ssh(node, f"/tmp/cca_part4_setup_mcperf.sh{setup_arg}", retries=2)


def refresh_controller_files(mem_node: str) -> None:
    scp_to(mem_node, ROOT / "part4_controller.py", "/tmp/cca_part4_controller.py")
    scp_to(mem_node, ROOT / "scheduler_logger.py", "/tmp/scheduler_logger.py")
    ssh(mem_node, "chmod +x /tmp/cca_part4_controller.py")


def stop_mcperf(agent_node: str, measure_node: str) -> None:
    for node in (agent_node, measure_node):
        ssh(node, "pkill -x mcperf || true", retries=1)


def cleanup_before_run(mem_node: str, agent_node: str, measure_node: str) -> None:
    stop_mcperf(agent_node, measure_node)
    ssh(
        mem_node,
        "containers=$(sudo docker ps -aq --filter name=cca-part4-); "
        "if [ -n \"$containers\" ]; then sudo docker rm -f $containers; fi; true",
        retries=1,
    )


def start_mcperf(
    measure_node: str,
    agent_node: str,
    memcached_ip: str,
    agent_ip: str,
    duration: int,
    qps_interval: int,
    qps_min: int,
    qps_max: int,
    out_file: Path,
) -> subprocess.Popen:
    ssh(agent_node, f"pkill -x mcperf 2>/dev/null || true; nohup {MCPERF_BIN} -T 8 -A >/tmp/mcperf-agent.log 2>&1 & disown; true")
    time.sleep(5)
    ssh(measure_node, f"pkill -x mcperf 2>/dev/null || true; {MCPERF_BIN} -s {memcached_ip} --loadonly")

    extra_args = os.environ.get("MCPERF_EXTRA_ARGS")
    if extra_args:
        load_args = extra_args
    else:
        load_args = f"--qps_interval {qps_interval} --qps_min {qps_min} --qps_max {qps_max}"

    command = (
        f"timeout {duration + 60}s {MCPERF_BIN} -s {memcached_ip} -a {agent_ip} "
        f"--noload -T 8 -C 8 -D 4 -Q 1000 -c 8 -t {duration} {load_args}"
    )
    return ssh_popen(measure_node, command, out_file)


def start_controller(
    mem_node: str,
    policy: str,
    run_id: int,
    mcperf_duration: int,
    max_runtime: int,
    drain_memcached_cores: int,
    sample_interval: float,
    debug_dir: Path,
) -> tuple[subprocess.Popen, str]:
    remote_log = f"/tmp/cca_part4_jobs_{policy}_{run_id}.txt"
    command = (
        f"cd /tmp && python3 /tmp/cca_part4_controller.py "
        f"--policy {policy} "
        f"--log-file {remote_log} "
        f"--mcperf-duration {mcperf_duration} "
        f"--max-runtime {max_runtime} "
        f"--drain-memcached-cores {drain_memcached_cores} "
        f"--sample-interval {sample_interval}"
    )
    debug_file = debug_dir / f"controller_{policy}_{run_id}.log"
    return ssh_popen(mem_node, command, debug_file), remote_log


def wait_for_processes(controller: subprocess.Popen, mcperf: subprocess.Popen, timeout: int) -> None:
    deadline = time.monotonic() + timeout
    while True:
        controller_rc = controller.poll()
        mcperf_rc = mcperf.poll()
        if controller_rc is not None and mcperf_rc is not None:
            if controller_rc != 0:
                raise RuntimeError(f"controller exited with {controller_rc}")
            if mcperf_rc != 0:
                raise RuntimeError(f"mcperf exited with {mcperf_rc}")
            return
        if time.monotonic() > deadline:
            for proc in (controller, mcperf):
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
            raise TimeoutError("Part 4 run exceeded local wait timeout")
        time.sleep(10)


def result_dir_for(policy: str, group: str) -> Path:
    question = "3" if policy == "static" else "4"
    return ROOT / f"part_4_{question}_results_group_{group}"


def run_one_policy(args: argparse.Namespace, policy: str, nodes: dict[str, str], ips: dict[str, str]) -> None:
    results_dir = result_dir_for(policy, args.group)
    results_dir.mkdir(exist_ok=True)
    debug_dir = ROOT / "part4_debug_logs"
    debug_dir.mkdir(exist_ok=True)

    for run_id in range(1, args.runs + 1):
        print(f"\n=== Part 4 {policy} run {run_id}/{args.runs} ===", flush=True)
        cleanup_before_run(nodes["memcached"], nodes["agent"], nodes["measure"])
        mcperf_file = results_dir / f"mcperf_{run_id}.txt"
        jobs_file = results_dir / f"jobs_{run_id}.txt"

        mcperf = start_mcperf(
            nodes["measure"],
            nodes["agent"],
            ips["memcached"],
            ips["agent"],
            args.duration,
            args.qps_interval,
            args.qps_min,
            args.qps_max,
            mcperf_file,
        )
        time.sleep(5)
        controller_runtime = args.duration + args.controller_grace
        controller, remote_log = start_controller(
            nodes["memcached"],
            policy,
            run_id,
            args.duration,
            controller_runtime,
            args.drain_memcached_cores,
            args.sample_interval,
            debug_dir,
        )

        try:
            wait_for_processes(controller, mcperf, controller_runtime + 300)
        finally:
            stop_mcperf(nodes["agent"], nodes["measure"])
            try:
                scp_from(nodes["memcached"], remote_log, jobs_file)
            except Exception as exc:
                print(f"warning: failed to copy {remote_log}: {exc}", file=sys.stderr)
            cleanup_before_run(nodes["memcached"], nodes["agent"], nodes["measure"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default="065", help="three-digit group number")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--policy-set", choices=["static", "dynamic", "both"], default="both")
    parser.add_argument("--duration", type=int, default=1800)
    parser.add_argument("--qps-interval", type=int, default=15)
    parser.add_argument("--qps-min", type=int, default=5000)
    parser.add_argument("--qps-max", type=int, default=110000)
    parser.add_argument("--sample-interval", type=float, default=5.0)
    parser.add_argument("--controller-grace", type=int, default=900, help="extra seconds for batch jobs after mcperf ends")
    parser.add_argument("--drain-memcached-cores", type=int, default=1)
    parser.add_argument("--setup", action="store_true", help="install/configure memcached, Docker, and mcperf first")
    args = parser.parse_args()

    if not re.fullmatch(r"\d{3}", args.group):
        raise SystemExit("--group must be a three-digit value such as 065")

    nodes = {
        "memcached": node_name("memcached"),
        "agent": node_name("client-agent"),
        "measure": node_name("client-measure"),
    }
    ips = {
        "memcached": node_internal_ip("memcached"),
        "agent": node_internal_ip("client-agent"),
        "measure": node_internal_ip("client-measure"),
    }

    if args.setup:
        setup_nodes(nodes["memcached"], nodes["agent"], nodes["measure"], ips["memcached"])
    else:
        refresh_controller_files(nodes["memcached"])

    policies = ["static", "dynamic"] if args.policy_set == "both" else [args.policy_set]
    for policy in policies:
        run_one_policy(args, policy, nodes, ips)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
