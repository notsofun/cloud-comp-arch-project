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
import subprocess
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

try:
    import docker
    from docker.errors import APIError, DockerException, NotFound
except ModuleNotFoundError:
    docker = None  # type: ignore[assignment]
    APIError = DockerException = NotFound = Exception  # type: ignore[misc,assignment]

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


class DockerRuntime:
    def __init__(self) -> None:
        if docker is None:
            raise RuntimeError("Docker Python SDK is not installed; run part4_setup_memcache.sh first")
        try:
            self.client = docker.from_env()
            self.client.ping()
        except DockerException as exc:
            raise RuntimeError(
                "failed to connect to Docker through the Python SDK; "
                "make sure docker is running and the ubuntu user is in the docker group"
            ) from exc

    def remove_matching(self, name_prefix: str) -> None:
        containers = self.client.containers.list(all=True, filters={"name": name_prefix})
        for container in containers:
            if container.name.startswith(name_prefix):
                try:
                    container.remove(force=True)
                except APIError:
                    pass

    def remove(self, name: str) -> None:
        try:
            self.client.containers.get(name).remove(force=True)
        except NotFound:
            pass

    def run_job(self, spec: BatchJob, cores: tuple[int, ...], name: str) -> None:
        self.remove(name)
        self.client.containers.run(
            spec.image,
            command=[
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
            ],
            cpuset_cpus=cpuset(cores),
            detach=True,
            name=name,
        )

    def state(self, name: str) -> tuple[str, int | None]:
        try:
            container = self.client.containers.get(name)
        except NotFound:
            return "missing", None
        container.reload()
        state: dict[str, Any] = container.attrs.get("State", {})
        return str(state.get("Status", "unknown")), state.get("ExitCode")

    def logs_tail(self, name: str, tail: int = 100) -> str:
        try:
            logs = self.client.containers.get(name).logs(tail=tail)
        except (APIError, NotFound):
            return ""
        return logs.decode("utf-8", errors="replace")[-2000:]

    def pause(self, name: str) -> None:
        self.client.containers.get(name).pause()

    def unpause(self, name: str) -> None:
        self.client.containers.get(name).unpause()

    def update_cpuset(self, name: str, cores: tuple[int, ...]) -> None:
        self.client.containers.get(name).update(cpuset_cpus=cpuset(cores))


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


def cleanup_containers(runtime: DockerRuntime) -> None:
    runtime.remove_matching("cca-part4-")


def start_container(
    runtime: DockerRuntime,
    spec: BatchJob,
    cores: tuple[int, ...],
    logger: SchedulerLogger,
) -> RunningJob:
    name = f"cca-part4-{spec.name}"
    runtime.run_job(spec, cores, name)
    logger.job_start(log_job(spec.name), core_list(cores), spec.threads)
    return RunningJob(spec=spec, container=name, cores=cores)


def container_done(runtime: DockerRuntime, job: RunningJob) -> bool:
    status, exit_code = runtime.state(job.container)
    if status == "missing":
        return False
    if status == "exited":
        if exit_code != 0:
            logs = runtime.logs_tail(job.container)
            raise RuntimeError(f"{job.container} exited with {exit_code}\n{logs}")
        return True
    return False


def update_container_cores(
    runtime: DockerRuntime,
    job: RunningJob,
    cores: tuple[int, ...],
    logger: SchedulerLogger,
) -> RunningJob:
    if not cores:
        if not job.paused:
            runtime.pause(job.container)
            logger.job_pause(log_job(job.spec.name))
            job.paused = True
        return job

    if job.cores != cores:
        runtime.update_cpuset(job.container, cores)
        logger.update_cores(log_job(job.spec.name), core_list(cores))
        job.cores = cores

    if job.paused:
        runtime.unpause(job.container)
        logger.job_unpause(log_job(job.spec.name))
        job.paused = False
    return job


def finish_container(runtime: DockerRuntime, job: RunningJob, logger: SchedulerLogger) -> None:
    logger.job_end(log_job(job.spec.name))
    runtime.remove(job.container)


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


def choose_drain_batch_cores(job: BatchJob, memcached_count: int) -> tuple[int, ...]:
    leftover = ALL_CORES[memcached_count:]
    if not leftover:
        return ()
    return tuple(leftover)


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
    runtime = DockerRuntime()
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
    drain_mode = False

    try:
        cleanup_containers(runtime)
        set_memcached_cores(memcached_cores)
        logger.job_start(LogJob.MEMCACHED, core_list(memcached_cores), args.memcached_threads)
        logger.custom_event(LogJob.SCHEDULER, f"policy={args.policy}")

        while True:
            now = time.monotonic()
            mcperf_done = now - start_time > args.mcperf_duration
            if mcperf_done and not pending and running is None:
                break
            if now - start_time > args.max_runtime:
                if pending or running is not None:
                    raise TimeoutError(f"scheduler exceeded max runtime of {args.max_runtime}s")
                break

            sample = monitor.sample()
            window.add(sample)

            if mcperf_done:
                desired_count = max(1, min(len(ALL_CORES), args.drain_memcached_cores))
                pressure_score = 0
                reason = "drain-after-mcperf"
                if not drain_mode:
                    logger.custom_event(
                        LogJob.SCHEDULER,
                        f"drain_after_mcperf memcached_cores={desired_count}",
                    )
                    drain_mode = True
            elif args.policy == "static":
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
            if desired_count > memcached_count:
                if running is not None:
                    if drain_mode:
                        desired_batch_cores = choose_drain_batch_cores(running.spec, desired_count)
                    else:
                        desired_batch_cores = choose_batch_cores(
                            args.policy,
                            running.spec,
                            desired_count,
                            pressure_score,
                            running=True,
                        )
                    running = update_container_cores(runtime, running, desired_batch_cores, logger)
                set_memcached_cores(desired_memcached_cores, logger)
            elif desired_count < memcached_count:
                set_memcached_cores(desired_memcached_cores, logger)
                if running is not None:
                    if drain_mode:
                        desired_batch_cores = choose_drain_batch_cores(running.spec, desired_count)
                    else:
                        desired_batch_cores = choose_batch_cores(
                            args.policy,
                            running.spec,
                            desired_count,
                            pressure_score,
                            running=True,
                        )
                    running = update_container_cores(runtime, running, desired_batch_cores, logger)
            elif running is not None:
                if drain_mode:
                    desired_batch_cores = choose_drain_batch_cores(running.spec, desired_count)
                else:
                    desired_batch_cores = choose_batch_cores(
                        args.policy,
                        running.spec,
                        desired_count,
                        pressure_score,
                        running=True,
                    )
                running = update_container_cores(runtime, running, desired_batch_cores, logger)
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

            if running is not None and container_done(runtime, running):
                finish_container(runtime, running, logger)
                running = None

            if running is None and pending:
                if drain_mode:
                    next_job = pending[0]
                    batch_cores = choose_drain_batch_cores(next_job, memcached_count)
                else:
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
                    running = start_container(runtime, next_job, batch_cores, logger)

            time.sleep(args.sample_interval)
    except Exception as exc:
        logger.custom_event(LogJob.SCHEDULER, f"error={exc}")
        if running is not None:
            runtime.remove(running.container)
            logger.job_end(log_job(running.spec.name))
        raise
    finally:
        logger.end()
        cleanup_containers(runtime)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", choices=["static", "dynamic"], required=True)
    parser.add_argument("--log-file", required=True)
    parser.add_argument("--max-runtime", type=int, default=1800)
    parser.add_argument("--mcperf-duration", type=int, default=1800)
    parser.add_argument("--drain-memcached-cores", type=int, default=1)
    parser.add_argument("--sample-interval", type=float, default=5.0)
    parser.add_argument("--memcached-threads", type=int, default=4)
    args = parser.parse_args()

    run_scheduler(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
