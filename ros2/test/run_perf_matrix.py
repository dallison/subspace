#!/usr/bin/env python3
"""Run ROS 2 local-SHM performance comparisons for Subspace and Iceoryx."""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import escape


@dataclass(frozen=True)
class Scenario:
    mode: str
    message: str
    subscribers: int
    history_depth: int
    runtime_s: int = 6
    ignore_s: int = 1
    rate: int = 0
    publishers: int = 1
    group: str = "baseline"
    subspace_slot_size: int | None = None


def shell_source_prefix(workspace: Path | None) -> str:
    parts = [". /opt/ros/jazzy/setup.bash"]
    if workspace is not None:
        parts.append(f". {workspace}/install/setup.bash")
    return "; ".join(parts)


def backend_environment(backend: str) -> dict[str, str]:
    env = os.environ.copy()
    if backend == "iceoryx":
        # The benchmark script is often launched from a sourced Subspace overlay.
        # Stock Iceoryx comparisons must start from the system ROS install only.
        for key in [
            "AMENT_PREFIX_PATH",
            "CMAKE_PREFIX_PATH",
            "COLCON_PREFIX_PATH",
            "LD_LIBRARY_PATH",
            "PYTHONPATH",
            "SUBSPACE_SOURCE_DIR",
        ]:
            env.pop(key, None)
    env["RMW_IMPLEMENTATION"] = "rmw_cyclonedds_cpp"
    return env


def start_process(command: str, env: dict[str, str], log_path: Path) -> subprocess.Popen:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log = log_path.open("w")
    return subprocess.Popen(
        ["bash", "-lc", command],
        stdout=log,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        start_new_session=True,
    )


def stop_process(process: subprocess.Popen | None) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=5)
    except Exception:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except Exception:
            pass


def read_process_memory_kb(pid: int) -> dict[str, int]:
    status_path = Path(f"/proc/{pid}/status")
    if not status_path.exists():
        return {}
    memory: dict[str, int] = {}
    for line in status_path.read_text(errors="replace").splitlines():
        if not line.startswith(("VmRSS:", "VmSize:", "VmHWM:", "VmPeak:")):
            continue
        parts = line.split()
        if len(parts) >= 2:
            try:
                memory[parts[0].rstrip(":").lower() + "_kb"] = int(parts[1])
            except ValueError:
                pass
    return memory


def parse_perf_csv(path: Path) -> tuple[dict[str, float], list[dict[str, float]]]:
    lines = path.read_text(errors="replace").splitlines()
    header_index = next(i for i, line in enumerate(lines) if line.startswith("T_experiment"))
    rows: list[dict[str, float]] = []
    reader = csv.reader(lines[header_index:])
    header = [item.strip() for item in next(reader)]
    for raw in reader:
        if not raw:
            continue
        values = [item.strip() for item in raw]
        if len(values) != len(header):
            continue
        row: dict[str, float] = {}
        for key, value in zip(header, values):
            if not value:
                continue
            try:
                row[key] = float(value)
            except ValueError:
                pass
        if row:
            rows.append(row)

    total_time = sum(row.get("T_loop", 0.0) for row in rows)
    total_received = sum(row.get("received", 0.0) for row in rows)
    total_sent = sum(row.get("sent", 0.0) for row in rows)
    total_lost = sum(row.get("lost", 0.0) for row in rows)
    total_data = sum(row.get("data_received", 0.0) for row in rows)

    weighted_latency = 0.0
    latency_weight = 0.0
    latency_mins: list[float] = []
    latency_maxs: list[float] = []
    for row in rows:
        received = row.get("received", 0.0)
        latency_mean = row.get("latency_mean (ms)")
        if latency_mean is not None and received > 0:
            weighted_latency += latency_mean * received
            latency_weight += received
        latency_min = row.get("latency_min (ms)")
        latency_max = row.get("latency_max (ms)")
        if latency_min is not None and latency_min < 1e12:
            latency_mins.append(latency_min)
        if latency_max is not None and latency_max > -1e12:
            latency_maxs.append(latency_max)

    summary = {
        "samples": total_received,
        "sent": total_sent,
        "lost": total_lost,
        "duration_s": total_time,
        "msg_per_s": total_received / total_time if total_time else 0.0,
        "sent_per_s": total_sent / total_time if total_time else 0.0,
        "mib_per_s": total_data / total_time / (1024.0 * 1024.0) if total_time else 0.0,
        "latency_mean_ms": weighted_latency / latency_weight if latency_weight else 0.0,
        "latency_min_ms": min(latency_mins) if latency_mins else 0.0,
        "latency_max_ms": max(latency_maxs) if latency_maxs else 0.0,
        "cpu_usage_pct_mean": (
            sum(row.get("cpu_usage (%)", 0.0) for row in rows) / len(rows) if rows else 0.0
        ),
        "maxrss_kb_max": max((row.get("ru_maxrss", 0.0) for row in rows), default=0.0),
        "maxrss_kb_mean": (
            sum(row.get("ru_maxrss", 0.0) for row in rows) / len(rows) if rows else 0.0
        ),
        "rows": len(rows),
    }
    return summary, rows


def combine_perf_summaries(summaries: list[dict[str, float]]) -> dict[str, float]:
    if not summaries:
        return {}
    duration = max((summary.get("duration_s", 0.0) for summary in summaries), default=0.0)
    samples = sum(summary.get("samples", 0.0) for summary in summaries)
    sent = sum(summary.get("sent", 0.0) for summary in summaries)
    lost = sum(summary.get("lost", 0.0) for summary in summaries)
    mib_per_s = sum(summary.get("mib_per_s", 0.0) for summary in summaries)
    rows = sum(summary.get("rows", 0.0) for summary in summaries)
    return {
        "samples": samples,
        "sent": sent,
        "lost": lost,
        "duration_s": duration,
        "msg_per_s": samples / duration if duration else 0.0,
        "sent_per_s": sent / duration if duration else 0.0,
        "mib_per_s": mib_per_s,
        "latency_mean_ms": 0.0,
        "latency_min_ms": 0.0,
        "latency_max_ms": 0.0,
        "cpu_usage_pct_mean": sum(
            summary.get("cpu_usage_pct_mean", 0.0) for summary in summaries
        ),
        "maxrss_kb_max": sum(summary.get("maxrss_kb_max", 0.0) for summary in summaries),
        "maxrss_kb_mean": sum(summary.get("maxrss_kb_mean", 0.0) for summary in summaries),
        "rows": rows,
    }


def message_size_bytes(message: str) -> int:
    suffixes = {
        "k": 1024,
        "m": 1024 * 1024,
    }
    if message.startswith("Array"):
        size = message.removeprefix("Array").lower()
        for suffix, multiplier in suffixes.items():
            if size.endswith(suffix):
                return int(size[: -len(suffix)]) * multiplier
        return int(size)
    if message.startswith("Struct"):
        size = message.removeprefix("Struct").lower()
        for suffix, multiplier in suffixes.items():
            if size.endswith(suffix):
                return int(size[: -len(suffix)]) * multiplier
        return int(size)
    if message.startswith("PointCloud"):
        size = message.removeprefix("PointCloud").lower()
        for suffix, multiplier in suffixes.items():
            if size.endswith(suffix):
                return int(size[: -len(suffix)]) * multiplier
    return 256


def write_subspace_config(path: Path, scenario: Scenario) -> None:
    slot_size = scenario.subspace_slot_size
    if slot_size is None:
        slot_size = max(256, message_size_bytes(scenario.message) + 512)
    max_active_messages = max(64, scenario.history_depth + 2)
    # Subspace reserves slots for publisher/subscriber bookkeeping in addition
    # to active subscriber messages. Keep enough headroom to avoid drops without
    # falling back to the oversized 1024-slot style defaults.
    slots = max(
        64,
        scenario.subscribers * (2 * max_active_messages - 2) + scenario.subscribers + 5,
    )
    path.write_text(
        '<CycloneDDS xmlns="https://cdds.io/config"><Domain Id="any">'
        "<SharedMemory><Enable>true</Enable><LogLevel>warn</LogLevel>"
        "<Subspace>"
        "<Socket>/tmp/subspace</Socket>"
        f"<SlotSize>{slot_size}</SlotSize>"
        f"<Slots>{slots}</Slots>"
        f"<MaxActiveMessages>{max_active_messages}</MaxActiveMessages>"
        f"<ExpectedSubscribers>{scenario.subscribers}</ExpectedSubscribers>"
        "<FixedSize>false</FixedSize>"
        "<Reliable>false</Reliable>"
        "<LogDroppedMessages>false</LogDroppedMessages>"
        "<DetectDroppedMessages>false</DetectDroppedMessages>"
        "</Subspace>"
        "</SharedMemory></Domain></CycloneDDS>\n"
    )


def run_trial(
    backend: str,
    scenario: Scenario,
    output_dir: Path,
    workspace: Path,
    domain_id: int,
) -> dict[str, object]:
    trial_name = (
        f"{backend}_{scenario.group}_{scenario.mode}_{scenario.message}"
        f"_pubs{scenario.publishers}_subs{scenario.subscribers}"
        f"_depth{scenario.history_depth}"
    )
    trial_dir = output_dir / trial_name
    trial_dir.mkdir(parents=True, exist_ok=True)

    env = backend_environment(backend)
    env["ROS_DOMAIN_ID"] = str(domain_id)
    if backend == "subspace":
        subspace_config = trial_dir / "cyclonedds_subspace.xml"
        write_subspace_config(subspace_config, scenario)
        env["CYCLONEDDS_URI"] = f"file://{escape(str(subspace_config))}"
    else:
        env["CYCLONEDDS_URI"] = f"file://{output_dir / 'cyclonedds_iceoryx.xml'}"

    source_prefix = shell_source_prefix(workspace if backend == "subspace" else None)
    daemon: subprocess.Popen | None = None
    subscriber: subprocess.Popen | None = None
    publishers: list[subprocess.Popen] = []
    try:
        if backend == "subspace":
            server = workspace / "install/subspace_vendor/lib/subspace_vendor/subspace_server"
            daemon = start_process(
                f"{source_prefix}; exec {server}",
                env,
                trial_dir / "daemon.log",
            )
        else:
            daemon = start_process(
                f"{source_prefix}; exec iox-roudi",
                env,
                trial_dir / "daemon.log",
            )
        time.sleep(2.0)
        if daemon.poll() is not None:
            raise RuntimeError(f"{backend} daemon exited early with {daemon.returncode}")

        common_args = (
            f"--msg {scenario.message} --max-runtime {scenario.runtime_s} "
            f"--ignore {scenario.ignore_s} --history-depth {scenario.history_depth} "
            "--history KEEP_LAST --reliability BEST_EFFORT "
            f"--dds-domain-id {domain_id} --topic {trial_name}"
        )
        if backend == "subspace":
            # performance_test's --shared-memory flag overwrites CYCLONEDDS_URI with
            # a minimal CycloneDDS config. For Subspace, keep our XML as the source
            # of truth and only request loaned samples when needed.
            shm_args = "--loaned-samples" if scenario.mode == "loaned" else ""
        else:
            shm_args = "--zero-copy" if scenario.mode == "loaned" else "--shared-memory"
        sub_csv = trial_dir / "subscriber.csv"
        sub_cmd = (
            f"{source_prefix}; exec ros2 run performance_test perf_test "
            f"-p 0 -s {scenario.subscribers} "
            "--expected-num-pubs 1 --expected-num-subs 0 "
            f"{common_args} {shm_args} --logfile {sub_csv}"
        )

        subscriber = start_process(sub_cmd, env, trial_dir / "subscriber.log")
        time.sleep(1.0)
        pub_csvs: list[Path] = []
        for publisher_index in range(scenario.publishers):
            pub_csv = (
                trial_dir / "publisher.csv"
                if scenario.publishers == 1
                else trial_dir / f"publisher_{publisher_index}.csv"
            )
            pub_csvs.append(pub_csv)
            pub_cmd = (
                f"{source_prefix}; exec ros2 run performance_test perf_test "
                "-p 1 -s 0 --expected-num-pubs 0 "
                f"--expected-num-subs {scenario.subscribers} "
                f"--rate {scenario.rate} {common_args} {shm_args} --logfile {pub_csv}"
            )
            publishers.append(
                start_process(pub_cmd, env, trial_dir / f"publisher_{publisher_index}.log")
            )
        time.sleep(min(1.0, max(0.0, scenario.runtime_s / 2.0)))
        daemon_memory_kb = read_process_memory_kb(daemon.pid)
        pub_rcs = [
            publisher.wait(timeout=scenario.runtime_s + 20) for publisher in publishers
        ]
        sub_rc = subscriber.wait(timeout=scenario.runtime_s + 20)
        if any(rc != 0 for rc in pub_rcs) or sub_rc != 0:
            raise RuntimeError(
                f"perf_test failed: publishers={pub_rcs} subscriber={sub_rc}"
            )

        sub_summary, sub_rows = parse_perf_csv(sub_csv)
        pub_summary = combine_perf_summaries(
            [parse_perf_csv(pub_csv)[0] for pub_csv in pub_csvs]
        )
        result: dict[str, object] = {
            "backend": backend,
            "group": scenario.group,
            "mode": scenario.mode,
            "message": scenario.message,
            "publishers": scenario.publishers,
            "subscribers": scenario.subscribers,
            "history_depth": scenario.history_depth,
            "runtime_s": scenario.runtime_s,
            "ignore_s": scenario.ignore_s,
            "domain_id": domain_id,
            "status": "ok",
            "subscriber": sub_summary,
            "publisher": pub_summary,
            "daemon_memory_kb": daemon_memory_kb,
            "logs": str(trial_dir),
        }
        if not sub_rows or sub_summary["samples"] <= 0:
            result["status"] = "no_samples"
        return result
    except Exception as exc:
        return {
            "backend": backend,
            "group": scenario.group,
            "mode": scenario.mode,
            "message": scenario.message,
            "publishers": scenario.publishers,
            "subscribers": scenario.subscribers,
            "history_depth": scenario.history_depth,
            "runtime_s": scenario.runtime_s,
            "ignore_s": scenario.ignore_s,
            "domain_id": domain_id,
            "status": "error",
            "error": str(exc),
            "logs": str(trial_dir),
        }
    finally:
        for publisher in publishers:
            stop_process(publisher)
        stop_process(subscriber)
        stop_process(daemon)


def default_scenarios() -> list[Scenario]:
    scenarios: list[Scenario] = []
    for message in ["Array128", "Array4k", "Array64k", "Array1m"]:
        for subscribers in [1, 4]:
            for depth in [1, 16, 64]:
                scenarios.append(Scenario("copy", message, subscribers, depth))
    for message in ["Array1k", "Array64k", "Array1m"]:
        for depth in [16, 64]:
            scenarios.append(Scenario("loaned", message, 1, depth))
    return scenarios


def quick_scenarios() -> list[Scenario]:
    return [
        Scenario("copy", "Array128", 1, 16, runtime_s=4),
        Scenario("copy", "Array64k", 1, 16, runtime_s=4),
        Scenario("loaned", "Array64k", 1, 16, runtime_s=4),
    ]


def fan_scenarios(runtime_s: int = 4) -> list[Scenario]:
    scenarios: list[Scenario] = []
    for message in ["Array128", "Array64k"]:
        scenarios.extend(
            [
                Scenario(
                    "copy",
                    message,
                    1,
                    16,
                    runtime_s=runtime_s,
                    publishers=4,
                    group="fan_in",
                ),
                Scenario(
                    "copy",
                    message,
                    4,
                    16,
                    runtime_s=runtime_s,
                    publishers=1,
                    group="fan_out",
                ),
                Scenario(
                    "copy",
                    message,
                    4,
                    16,
                    runtime_s=runtime_s,
                    publishers=4,
                    group="fan_in_out",
                ),
            ]
        )
    scenarios.append(
        Scenario(
            "loaned",
            "Array64k",
            4,
            16,
            runtime_s=runtime_s,
            publishers=4,
            group="fan_in_out",
        )
    )
    return scenarios


def resize_memory_scenarios(runtime_s: int = 4) -> list[Scenario]:
    return [
        Scenario(
            "copy",
            message,
            1,
            16,
            runtime_s=runtime_s,
            group="resize_memory",
            subspace_slot_size=256,
        )
        for message in ["Array128", "Array4k", "Array64k", "Array1m"]
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, default=Path("/work/ws"))
    parser.add_argument("--output-dir", type=Path, default=Path("/work/ws/log/perf_matrix"))
    parser.add_argument("--backends", nargs="+", default=["subspace", "iceoryx"])
    parser.add_argument("--quick", action="store_true")
    parser.add_argument(
        "--fan",
        action="store_true",
        help="run fan-in/fan-out scenarios with multiple publishers/subscribers",
    )
    parser.add_argument(
        "--resize-memory",
        action="store_true",
        help="run increasing message-size scenarios and report memory metrics",
    )
    args = parser.parse_args()

    if shutil.which("bash") is None:
        raise SystemExit("bash is required")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "cyclonedds_iceoryx.xml").write_text(
        '<CycloneDDS xmlns="https://cdds.io/config"><Domain Id="any">'
        "<SharedMemory><Enable>true</Enable><LogLevel>warn</LogLevel></SharedMemory>"
        "</Domain></CycloneDDS>\n"
    )

    scenarios: list[Scenario] = []
    if args.fan:
        scenarios.extend(fan_scenarios())
    if args.resize_memory:
        scenarios.extend(resize_memory_scenarios())
    if not scenarios:
        scenarios = quick_scenarios() if args.quick else default_scenarios()
    elif args.quick:
        for scenario in scenarios:
            if scenario.runtime_s > 4:
                raise SystemExit("quick-specific scenarios should already use short runtimes")

    results: list[dict[str, object]] = []
    domain_id = 80
    total = len(args.backends) * len(scenarios)
    completed = 0
    for scenario in scenarios:
        for backend in args.backends:
            completed += 1
            print(
                f"[{completed}/{total}] {backend} {scenario.group} {scenario.mode} "
                f"{scenario.message} pubs={scenario.publishers} "
                f"subs={scenario.subscribers} depth={scenario.history_depth}",
                flush=True,
            )
            result = run_trial(backend, scenario, args.output_dir, args.workspace, domain_id)
            results.append(result)
            domain_id += 1
            metric = result.get("subscriber", {})
            if isinstance(metric, dict):
                print(
                    f"  {result['status']} {metric.get('msg_per_s', 0):.0f} msg/s "
                    f"{metric.get('mib_per_s', 0):.1f} MiB/s "
                    f"{metric.get('latency_mean_ms', 0):.4f} ms",
                    flush=True,
                )
            else:
                print(f"  {result['status']} {result.get('error', '')}", flush=True)

    output = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "environment": {
            "container": "ros:jazzy-ros-base",
            "platform": "linux/arm64",
            "workspace": str(args.workspace),
            "note": "Publisher and subscriber ran as separate processes with CycloneDDS shared memory enabled.",
        },
        "results": results,
    }
    results_path = args.output_dir / "results.json"
    results_path.write_text(json.dumps(output, indent=2, sort_keys=True))
    print(f"wrote {results_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
