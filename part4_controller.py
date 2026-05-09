#!/usr/bin/env python3
"""
Dynamic Docker scheduler for CCA Part 4.

This controller runs on the memcache-server VM. It keeps memcached pinned with
taskset, starts PARSEC containers with Docker, and changes container CPU sets
while mcperf is running. The "dynamic" policy is a proactive headroom policy:
it watches short-term CPU, softirq, load, and network trends and gives
memcached extra cores before the machine is saturated.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import time
from collections import deque
from dataclasses import dataclass

from scheduler_logger import Job as LogJob
from scheduler_logger import SchedulerLogger


ALL_CORES = (0, 1, 2, 3)


@dataclass(frozen=True)
class BatchJob:
    name: str
    image: str
    suite: str
    threads: int
    risk: str
    preferred_cores: int
    max_cores: int


@dataclass
class RunningJob:
    spec: BatchJob
    container: str
    cores: tuple[int, ...]
    paused: bool = False


@dataclass(frozen=True)
class ResourceSample:
    utilization: dict[int, float]
    softirq: dict[int, float]
    rx_mbps: float
    tx_mbps: float
    load1_normalized: float

    def avg_util(self, cores: tuple[int, ...]) -> float:
        return sum(self.utilization.get(core, 0.0) for core in cores) / max(1, len(cores))

    def max_util(self) -> float:
        return max((self.utilization.get(core, 0.0) for core in ALL_CORES), default=0.0)

    def avg_softirq(self, cores: tuple[int, ...]) -> float:
        return sum(self.softirq.get(core, 0.0) for core in cores) / max(1, len(cores))


JOB_QUEUE = [
    BatchJob("streamcluster", "anakli/cca:parsec_streamcluster", "parsec", 2, "high", 1, 2),
    BatchJob("freqmine", "anakli/cca:parsec_freqmine", "parsec", 2, "high", 1, 2),
    BatchJob("canneal", "anakli/cca:parsec_canneal", "parsec", 2, "high", 1, 2),
    BatchJob("vips", "anakli/cca:parsec_vips", "parsec", 2, "medium", 1, 2),
    BatchJob("barnes", "anakli/cca:splash2x_barnes", "splash2x", 2, "medium", 1, 2),
    BatchJob("blackscholes", "anakli/cca:parsec_blackscholes", "parsec", 2, "low", 1, 2),
    BatchJob("radix", "anakli/cca:splash2x_radix", "splash2x", 1, "low", 1, 1),
]


def run(cmd: list[str], *, capture: bool = False, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
    )


def docker(*args: str, capture: bool = False, check: bool = True) -> subprocess.CompletedProcess:
    base = ["docker"] if os.geteuid() == 0 else ["sudo", "docker"]
    return run([*base, *args], capture=capture, check=check)


def core_list(cores: tuple[int, ...] | list[int]) -> list[str]:
    return [str(core) for core in cores]


def cpuset(cores: tuple[int, ...] | list[int]) -> str:
    return ",".join(str(core) for core in cores)


def log_job(job_name: str) -> LogJob:
    return LogJob[job_name.upper()]


def memcached_pid() -> str:
    out = run(["pgrep", "-xo", "memcached"], capture=True).stdout.strip()
    if not out:
        raise RuntimeError("memcached is not running")
    return out


def set_memcached_cores(cores: tuple[int, ...], logger: SchedulerLogger | None = None) -> None:
    run(["sudo", "taskset", "-a", "-cp", cpuset(cores), memcached_pid()], capture=True)
    if logger is not None:
        logger.update_cores(LogJob.MEMCACHED, core_list(cores))


class SystemMonitor:
    def __init__(self) -> None:
        self.previous_cpu = self._read_cpu()
        self.previous_net = self._read_net()
        self.previous_time = time.monotonic()

    @staticmethod
    def _read_cpu() -> dict[int, tuple[int, int, int]]:
        stats: dict[int, tuple[int, int, int]] = {}
        with open("/proc/stat", "r", encoding="utf-8") as proc:
            for line in proc:
                if not line.startswith("cpu") or line.startswith("cpu "):
                    continue
                parts = line.split()
                core = int(parts[0][3:])
                values = [int(value) for value in parts[1:]]
                idle = values[3] + values[4]
                softirq = values[6] if len(values) > 6 else 0
                total = sum(values)
                stats[core] = (idle, total, softirq)
        return stats

    @staticmethod
    def _read_net() -> tuple[int, int]:
        rx_total = 0
        tx_total = 0
        with open("/proc/net/dev", "r", encoding="utf-8") as proc:
            for line in proc:
                if ":" not in line:
                    continue
                name, values = line.split(":", 1)
                if name.strip() == "lo":
                    continue
                fields = values.split()
                if len(fields) >= 16:
                    rx_total += int(fields[0])
                    tx_total += int(fields[8])
        return rx_total, tx_total

    @staticmethod
    def _load1_normalized() -> float:
        try:
            with open("/proc/loadavg", "r", encoding="utf-8") as proc:
                load1 = float(proc.read().split()[0])
            return load1 / len(ALL_CORES)
        except Exception:
            return 0.0

    def sample(self) -> ResourceSample:
        current_cpu = self._read_cpu()
        current_net = self._read_net()
        now = time.monotonic()
        elapsed = max(0.001, now - self.previous_time)

        utilization: dict[int, float] = {}
        softirq: dict[int, float] = {}
        for core, (idle, total, soft) in current_cpu.items():
            old_idle, old_total, old_soft = self.previous_cpu.get(core, (idle, total, soft))
            total_delta = max(1, total - old_total)
            idle_delta = max(0, idle - old_idle)
            soft_delta = max(0, soft - old_soft)
            utilization[core] = max(0.0, min(1.0, 1.0 - idle_delta / total_delta))
            softirq[core] = max(0.0, min(1.0, soft_delta / total_delta))

        rx_delta = max(0, current_net[0] - self.previous_net[0])
        tx_delta = max(0, current_net[1] - self.previous_net[1])
        self.previous_cpu = current_cpu
        self.previous_net = current_net
        self.previous_time = now

        return ResourceSample(
            utilization=utilization,
            softirq=softirq,
            rx_mbps=(rx_delta * 8) / (elapsed * 1_000_000),
            tx_mbps=(tx_delta * 8) / (elapsed * 1_000_000),
            load1_normalized=self._load1_normalized(),
        )


class PressureWindow:
    def __init__(self, size: int = 6) -> None:
        self.samples: deque[ResourceSample] = deque(maxlen=size)

    def add(self, sample: ResourceSample) -> None:
        self.samples.append(sample)

    def ready(self) -> bool:
        return len(self.samples) >= 2

    def avg_all(self, sample: ResourceSample | None = None) -> float:
        current = sample or self.samples[-1]
        return current.avg_util(ALL_CORES)

    def util_trend(self) -> float:
        if len(self.samples) < 4:
            return 0.0
        older = list(self.samples)[: len(self.samples) // 2]
        newer = list(self.samples)[len(self.samples) // 2 :]
        older_avg = sum(sample.avg_util(ALL_CORES) for sample in older) / len(older)
        newer_avg = sum(sample.avg_util(ALL_CORES) for sample in newer) / len(newer)
        return newer_avg - older_avg

    def rx_trend(self) -> float:
        if len(self.samples) < 4:
            return 0.0
        older = list(self.samples)[: len(self.samples) // 2]
        newer = list(self.samples)[len(self.samples) // 2 :]
        older_avg = sum(sample.rx_mbps for sample in older) / len(older)
        newer_avg = sum(sample.rx_mbps for sample in newer) / len(newer)
        return newer_avg - older_avg

    def pressure_score(self, sample: ResourceSample, memcached_count: int) -> tuple[int, str]:
        mem_cores = ALL_CORES[:memcached_count]
        avg_mem = sample.avg_util(mem_cores)
        avg_all = sample.avg_util(ALL_CORES)
        max_core = sample.max_util()
        avg_softirq = sample.avg_softirq(ALL_CORES)
        util_trend = self.util_trend()
        rx_trend = self.rx_trend()

        score = 0
        reasons: list[str] = []

        if avg_mem >= 0.70:
            score += 2
            reasons.append("mem-util-high")
        elif avg_mem >= 0.55:
            score += 1
            reasons.append("mem-util-rising")

        if avg_all >= 0.82:
            score += 2
            reasons.append("node-util-high")
        elif avg_all >= 0.68:
            score += 1
            reasons.append("node-util-rising")

        if max_core >= 0.88:
            score += 2
            reasons.append("hot-core")
        elif max_core >= 0.76:
            score += 1
            reasons.append("warm-core")

        if avg_softirq >= 0.12:
            score += 2
            reasons.append("softirq-high")
        elif avg_softirq >= 0.06:
            score += 1
            reasons.append("softirq-rising")

        if sample.load1_normalized >= 1.05:
            score += 2
            reasons.append("runqueue-high")
        elif sample.load1_normalized >= 0.82:
            score += 1
            reasons.append("runqueue-rising")

        if util_trend >= 0.10:
            score += 1
            reasons.append("cpu-trend-up")
        if rx_trend >= 12.0:
            score += 1
            reasons.append("network-trend-up")

        return score, ",".join(reasons) or "low-pressure"


class HeadroomPolicy:
    def __init__(self) -> None:
        self.low_pressure_samples = 0
        self.last_memcached_resize = 0.0

    def choose_memcached_count(
        self,
        current_count: int,
        sample: ResourceSample,
        window: PressureWindow,
        now: float,
    ) -> tuple[int, str, int]:
        score, reason = window.pressure_score(sample, current_count)

        if score >= 6:
            desired = 4
        elif score >= 3:
            desired = max(3, current_count)
        else:
            desired = current_count

        # Part 4 has a tight 0.8 ms p95 SLO. Keep at least two cores for
        # memcached and shrink only after repeated low-pressure samples.
        low_pressure = (
            score <= 1
            and sample.avg_util(ALL_CORES[:current_count]) <= 0.45
            and sample.avg_util(ALL_CORES) <= 0.58
            and sample.max_util() <= 0.70
            and sample.load1_normalized <= 0.70
        )
        if low_pressure:
            self.low_pressure_samples += 1
        else:
            self.low_pressure_samples = 0

        if (
            desired <= current_count
            and current_count > 2
            and self.low_pressure_samples >= 4
            and now - self.last_memcached_resize >= 30
        ):
            desired = current_count - 1
            reason = "stable-low-pressure"
            self.low_pressure_samples = 0

        desired = max(2, min(4, desired))
        if desired != current_count:
            self.last_memcached_resize = now
        return desired, reason, score


def cleanup_containers() -> None:
    out = docker("ps", "-a", "--filter", "name=cca-part4-", "--format", "{{.Names}}", capture=True, check=False).stdout
    names = [line.strip() for line in out.splitlines() if line.strip()]
    if names:
        docker("rm", "-f", *names, check=False)


def start_container(spec: BatchJob, cores: tuple[int, ...], logger: SchedulerLogger) -> RunningJob:
    name = f"cca-part4-{spec.name}"
    docker("rm", "-f", name, check=False)
    docker(
        "run",
        "--cpuset-cpus",
        cpuset(cores),
        "-d",
        "--name",
        name,
        spec.image,
        "./run",
        "-a",
        "run",
        "-S",
        spec.suite,
        "-p",
        spec.name,
        "-i",
        "native",
        "-n",
        str(spec.threads),
    )
    logger.job_start(log_job(spec.name), core_list(cores), spec.threads)
    return RunningJob(spec=spec, container=name, cores=cores)


def container_done(job: RunningJob) -> bool:
    out = docker(
        "inspect",
        "-f",
        "{{.State.Status}} {{.State.ExitCode}}",
        job.container,
        capture=True,
        check=False,
    )
    if out.returncode != 0:
        return False
    status, exit_code = out.stdout.strip().split()
    if status == "exited":
        if exit_code != "0":
            logs = docker("logs", job.container, capture=True, check=False).stdout[-2000:]
            raise RuntimeError(f"{job.container} exited with {exit_code}\n{logs}")
        return True
    return False


def update_container_cores(job: RunningJob, cores: tuple[int, ...], logger: SchedulerLogger) -> RunningJob:
    if not cores:
        if not job.paused:
            docker("pause", job.container)
            logger.job_pause(log_job(job.spec.name))
            job.paused = True
        return job

    if job.cores != cores:
        docker("container", "update", "--cpuset-cpus", cpuset(cores), job.container, capture=True)
        logger.update_cores(log_job(job.spec.name), core_list(cores))
        job.cores = cores

    if job.paused:
        docker("unpause", job.container)
        logger.job_unpause(log_job(job.spec.name))
        job.paused = False
    return job


def finish_container(job: RunningJob, logger: SchedulerLogger) -> None:
    logger.job_end(log_job(job.spec.name))
    docker("rm", job.container, check=False)


def static_memcached_count() -> int:
    return 3


def choose_batch_cores(
    policy: str,
    job: BatchJob,
    memcached_count: int,
    pressure_score: int,
    running: bool,
) -> tuple[int, ...]:
    leftover = ALL_CORES[memcached_count:]
    if not leftover:
        return ()
    if policy == "static":
        return leftover[:1]

    if pressure_score >= 6:
        return ()
    if pressure_score >= 4 and job.risk != "low":
        return leftover[:1] if running else ()
    if pressure_score >= 3 and job.risk == "high":
        return leftover[:1] if running else ()

    target = min(job.max_cores, len(leftover))
    if pressure_score >= 2 or job.risk == "high":
        target = min(target, job.preferred_cores)
    return tuple(leftover[:target])


def select_next_job(
    policy: str,
    pending: list[BatchJob],
    memcached_count: int,
    pressure_score: int,
    window: PressureWindow,
) -> tuple[BatchJob | None, tuple[int, ...]]:
    if policy == "static":
        job = pending[0]
        return job, choose_batch_cores(policy, job, memcached_count, pressure_score, running=False)

    for job in pending:
        cores = choose_batch_cores(policy, job, memcached_count, pressure_score, running=False)
        if not cores:
            continue
        if job.risk == "high" and (pressure_score >= 2 or not window.ready()):
            continue
        if job.risk == "medium" and pressure_score >= 4:
            continue
        return job, cores
    return None, ()


def run_scheduler(args: argparse.Namespace) -> None:
    logger = SchedulerLogger(args.log_file)
    pending = list(JOB_QUEUE)
    running: RunningJob | None = None
    memcached_count = static_memcached_count() if args.policy == "static" else 3
    memcached_cores = ALL_CORES[:memcached_count]
    monitor = SystemMonitor()
    window = PressureWindow()
    headroom = HeadroomPolicy()
    last_reason = ""
    last_score = -1
    start_time = time.monotonic()

    try:
        cleanup_containers()
        set_memcached_cores(memcached_cores)
        logger.job_start(LogJob.MEMCACHED, core_list(memcached_cores), args.memcached_threads)
        logger.custom_event(LogJob.SCHEDULER, f"policy={args.policy}")

        while pending or running is not None:
            now = time.monotonic()
            if now - start_time > args.max_runtime:
                raise TimeoutError(f"scheduler exceeded max runtime of {args.max_runtime}s")

            sample = monitor.sample()
            window.add(sample)

            if args.policy == "static":
                desired_count = static_memcached_count()
                pressure_score, reason = window.pressure_score(sample, desired_count)
            else:
                desired_count, reason, pressure_score = headroom.choose_memcached_count(
                    memcached_count,
                    sample,
                    window,
                    now,
                )

            desired_memcached_cores = ALL_CORES[:desired_count]
            if desired_count > memcached_count and running is not None:
                desired_batch_cores = choose_batch_cores(
                    args.policy,
                    running.spec,
                    desired_count,
                    pressure_score,
                    running=True,
                )
                running = update_container_cores(running, desired_batch_cores, logger)
                set_memcached_cores(desired_memcached_cores, logger)
            elif desired_count < memcached_count:
                set_memcached_cores(desired_memcached_cores, logger)
                if running is not None:
                    desired_batch_cores = choose_batch_cores(
                        args.policy,
                        running.spec,
                        desired_count,
                        pressure_score,
                        running=True,
                    )
                    running = update_container_cores(running, desired_batch_cores, logger)
            elif running is not None:
                desired_batch_cores = choose_batch_cores(
                    args.policy,
                    running.spec,
                    desired_count,
                    pressure_score,
                    running=True,
                )
                running = update_container_cores(running, desired_batch_cores, logger)
            memcached_count = desired_count

            if reason != last_reason or pressure_score != last_score:
                logger.custom_event(
                    LogJob.SCHEDULER,
                    (
                        f"score={pressure_score} reason={reason} "
                        f"memcached_cores={memcached_count} "
                        f"avg_all={sample.avg_util(ALL_CORES):.2f} "
                        f"max_core={sample.max_util():.2f} "
                        f"rx_mbps={sample.rx_mbps:.1f} "
                        f"load1_norm={sample.load1_normalized:.2f}"
                    ),
                )
                last_reason = reason
                last_score = pressure_score

            if running is not None and container_done(running):
                finish_container(running, logger)
                running = None

            if running is None and pending:
                next_job, batch_cores = select_next_job(
                    args.policy,
                    pending,
                    memcached_count,
                    pressure_score,
                    window,
                )
                if next_job is not None and batch_cores:
                    pending.remove(next_job)
                    logger.custom_event(
                        LogJob.SCHEDULER,
                        f"admit={next_job.name} risk={next_job.risk} score={pressure_score}",
                    )
                    running = start_container(next_job, batch_cores, logger)

            time.sleep(args.sample_interval)
    except Exception as exc:
        logger.custom_event(LogJob.SCHEDULER, f"error={exc}")
        if running is not None:
            docker("rm", "-f", running.container, check=False)
            logger.job_end(log_job(running.spec.name))
        raise
    finally:
        logger.end()
        cleanup_containers()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", choices=["static", "dynamic"], required=True)
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--max-runtime", type=int, default=1800)
    parser.add_argument("--sample-interval", type=float, default=5.0)
    parser.add_argument("--memcached-threads", type=int, default=4)
    args = parser.parse_args()

    run_scheduler(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
