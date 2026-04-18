#!/usr/bin/env python3

import re
from pathlib import Path

import matplotlib.pyplot as plt


def parse_result(path: Path):
    rows = []
    for line in path.read_text().splitlines():
        s = line.strip()
        if not s or s.startswith("#") or s.startswith("-"):
            continue
        # Columns are whitespace-separated:
        # clientCount latAvg latP50 latP90 latP99 throughput
        parts = re.split(r"\s+", s)
        if len(parts) < 6:
            continue
        rows.append(
            {
                "clientCount": int(parts[0]),
                "latAvg": float(parts[1]),
                "latP50": float(parts[2]),
                "latP90": float(parts[3]),
                "latP99": float(parts[4]),
                "throughput": float(parts[5]),
            }
        )
    return rows


def main():
    repo_root = Path(__file__).resolve().parents[1]
    result_path = repo_root / "build" / "app" / "result.txt"
    if not result_path.exists():
        raise SystemExit(
            f"Missing `result.txt` at `{result_path}` (run `tput` from `build/app/` or copy output there)."
        )

    rows = parse_result(result_path)
    if not rows:
        raise SystemExit("No data rows found in result.txt.")

    # Sort by throughput for nicer line shapes
    rows = sorted(rows, key=lambda r: r["throughput"])

    x = [r["throughput"] for r in rows]
    y_avg = [r["latAvg"] for r in rows]
    y_p50 = [r["latP50"] for r in rows]
    y_p90 = [r["latP90"] for r in rows]
    y_p99 = [r["latP99"] for r in rows]

    plt.figure(figsize=(9, 6))
    plt.plot(x, y_avg, marker="o", label="latAvg")
    plt.plot(x, y_p50, marker="o", label="latP50")
    plt.plot(x, y_p90, marker="o", label="latP90")
    plt.plot(x, y_p99, marker="o", label="latP99")

    plt.xlabel("Throughput (ops/sec)")
    plt.ylabel("Latency (ms)")
    plt.title("Latency vs Throughput")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.legend()

    out_path = repo_root / "bench" / "lat-tput.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    print(f"Wrote plot to `{out_path}`")


if __name__ == "__main__":
    main()

