"""Render the #658 writer benchmark tables from its JSON results.

Usage: python3 docs/audit/658-live-flush-report.py docs/audit/658-live-flush-benchmark.json
(or one or more `bench_live_flush matrix --results` JSON-lines files).
"""
import json
import sys

STORAGES = ["local", "s3-0ms", "s3-10ms", "s3-30ms", "s3-80ms", "s3-30ms-slow200"]
CHAINS = [("robinhood", "Robinhood (10 blocks/s)"), ("arbitrum", "Arbitrum One (4 blocks/s)")]


class Results(dict):
    """First run of each label; `rounds(label)` lists every run of it."""

    def __init__(self, runs):
        super().__init__()
        self.all = {}
        for run in runs:
            label = run["scenario"]["label"]
            self.all.setdefault(label, []).append(run)
            self.setdefault(label, run)

    def rounds(self, label):
        return self.all.get(label, [])


IN_PROCESS = []


def load(paths):
    results = []
    for path in paths:
        text = open(path).read()
        if path.endswith(".json"):
            doc = json.loads(text)
            results.extend(doc["scenarios"])
            IN_PROCESS.extend(doc.get("in_process_phases", []))
        else:
            results.extend(json.loads(line) for line in text.splitlines() if line.strip())
    return Results(r for r in results if "error" not in r)


def rounds(results, label, *path, scale=1.0, digits=1):
    """`first / second` values of a metric when a label ran twice."""
    path = path or ("blocks_per_second",)
    runs = results.rounds(label)
    values = [get(r, *path) for r in runs]
    return " / ".join(num(v / scale, digits) for v in values if v is not None) or "-"


def get(result, *path):
    value = result
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def sec(value):
    return "-" if value is None else f"{value / 1000:.2f}"


def num(value, digits=1):
    return "-" if value is None else f"{value:,.{digits}f}"


def fired(result):
    return ", ".join(f"{k} {v}" for k, v in sorted(result["triggers"].items()))


def multiple(results, label):
    """The lower of the rounds' chain-rate multiples."""
    runs = results.rounds(label)
    return f"{min(r['chain_rate_multiple'] for r in runs):,.1f}x" if runs else "-"


def catchup(results):
    out = []
    modes = [("catchup-size32m", "256 MiB (default)"), ("catchup-size32m-mem1g", "1 GiB")]
    for chain, title in CHAINS:
        out.append(f"\n#### {title}: catch-up, size-only flushes (`--flush-bytes 32 MiB`)\n")
        out.append(
            "| Storage | Memory trigger | Blocks/s, rounds 1 / 2 | x chain (lower) | Blocks per commit "
            "| Objects per 1,000 blocks | Bytes per object | Commit p50 / p99 (s) | Mapping per window (s) "
            "| Peak RSS (MiB) |"
        )
        out.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
        for mode, name in modes:
            for storage in STORAGES:
                label = f"{chain}/{storage}/{mode}"
                r = results.get(label)
                if not r:
                    continue
                per_k = r["objects"] / r["blocks"] * 1000 if r["blocks"] else None
                out.append(
                    f"| {storage} | {name} | {rounds(results, label)} | {multiple(results, label)} "
                    f"| {num(r['blocks_per_commit'], 0)} | {num(per_k)} | {num(r['bytes_per_object'], 0)} "
                    f"| {sec(r['commit_ms']['p50'])} / {sec(r['commit_ms']['p99'])} "
                    f"| {sec(get(r, 'between_commits_ms', 'p50'))} | {num(r.get('max_rss_mib'), 0)} |"
                )
    return out


def windows(results):
    out = []
    for chain, title in CHAINS:
        out.append(f"\n#### {title}: back-to-back commits of 60 s, 120 s and 300 s windows\n")
        out.append(
            "| Storage | Window | Blocks per commit | Commit p50 / p99 (s) | Mapping per window (s) "
            "| Blocks/s, rounds | x chain = margin (lower) | Peak RSS (MiB) |"
        )
        out.append("|---|---|---:|---:|---:|---:|---:|---:|")
        for mode in ("windows-60s", "windows-120s", "windows-300s"):
            for storage in STORAGES:
                label = f"{chain}/{storage}/{mode}"
                r = results.get(label)
                if not r:
                    continue
                out.append(
                    f"| {storage} | {mode[8:]} | {num(r['blocks_per_commit'], 0)} "
                    f"| {sec(r['commit_ms']['p50'])} / {sec(r['commit_ms']['p99'])} "
                    f"| {sec(get(r, 'between_commits_ms', 'p50'))} | {rounds(results, label)} "
                    f"| {multiple(results, label)} | {num(r.get('max_rss_mib'), 0)} |"
                )
    return out


def steady(results):
    out = [
        "",
        "| Chain | Storage | Interval | Triggers (fired) | Flush period mean (s) | Blocks per commit "
        "| Commit p50 / max (s) | Commit share of period | Lag p50 / max (s) |",
        "|---|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    for chain, title in CHAINS:
        for mode in ("steady-120s", "steady-60s"):
            for storage in STORAGES:
                r = results.get(f"{chain}/{storage}/{mode}")
                if not r:
                    continue
                period = r["flush_period_s"]["mean"]
                share = r["commit_ms"]["mean"] / 1000 / period if period else None
                out.append(
                    f"| {title.split(' (')[0]} | {storage} | {mode[7:]} | {fired(r)} | {num(period, 1)} "
                    f"| {num(r['blocks_per_commit'], 0)} "
                    f"| {sec(r['commit_ms']['p50'])} / {sec(r['commit_ms']['max'])} "
                    f"| {num(share * 100 if share is not None else None)}% "
                    f"| {sec(get(r, 'lag_ms', 'p50'))} / {sec(get(r, 'lag_ms', 'max'))} |"
                )
    return out


PHASES = [
    "prepare",
    "unoccupied_heads",
    "writing",
    "table_work",
    "final_verify",
    "committed",
    "authority",
    "mirror",
    "clear",
    "tail",
]


def phase_table(results, mode):
    out = []
    for chain, title in CHAINS:
        storages = [s for s in STORAGES if s.startswith("s3") and f"{chain}/{s}/{mode}" in results]
        if not storages:
            continue
        out.append(f"\n#### {title}, `{mode}`: phase p50 / p99 (ms)\n")
        out.append("| Phase | " + " | ".join(storages) + " |")
        out.append("|---|" + "---:|" * len(storages))
        for phase in PHASES + ["commit total"]:
            cells = []
            for storage in storages:
                r = results[f"{chain}/{storage}/{mode}"]
                v = r["commit_ms"] if phase == "commit total" else get(r, "phases_ms", phase)
                cells.append("-" if not v else f"{v['p50']:.0f} / {v['p99']:.0f}")
            out.append(f"| {phase} | " + " | ".join(cells) + " |")
        for label, path, digits in [
            ("requests per commit (mean)", ("requests_per_commit", "mean"), 0),
            ("receipt writes per commit", ("requests_per_commit_by_class", "receipts_put"), 1),
            ("parts per commit", ("files_per_commit", "mean"), 1),
            ("server time per request (ms, mean)", ("request_ms_by_class_mean", "control_read"), 1),
        ]:
            cells = [num(get(results[f"{chain}/{s}/{mode}"], *path), digits) for s in storages]
            out.append(f"| {label} | " + " | ".join(cells) + " |")
    return out


def tuning(results):
    out = []
    for chain, title in CHAINS:
        out.append(f"\n#### {title}: publication concurrency and `--cursor none`\n")
        out.append(
            "| Storage | Setting | Catch-up blocks/s | Catch-up commit p50 (s) "
            "| 60 s windows blocks/s | 60 s windows commit p50 (s) |"
        )
        out.append("|---|---|---:|---:|---:|---:|")
        for storage in ("s3-30ms", "s3-80ms"):
            for variant, label in [
                ("", "default (encode 2, publish 4), rounds 1 / 2"),
                ("/e2p8", "encode 2, publish 8"),
                ("/e2p16", "encode 2, publish 16"),
                ("/e4p16", "encode 4, publish 16"),
                ("nocursor", "defaults, `--cursor none`"),
            ]:
                if variant == "nocursor":
                    c, w = f"{chain}/{storage}/catchup-size32m-nocursor", None
                else:
                    c = f"{chain}/{storage}/catchup-size32m{variant}"
                    w = f"{chain}/{storage}/windows-60s{variant}"
                if not results.rounds(c):
                    continue
                cells = [
                    rounds(results, c),
                    rounds(results, c, "commit_ms", "p50", scale=1000, digits=2),
                    rounds(results, w) if w else "-",
                    rounds(results, w, "commit_ms", "p50", scale=1000, digits=2) if w else "-",
                ]
                out.append(f"| {storage} | {label} | " + " | ".join(cells) + " |")
    return out


def in_process():
    if not IN_PROCESS:
        return []
    columns = [
        ("begin (plan, unoccupied checks, Writing)", "begin"),
        ("table_work (encode, stage, receipts, publish)", "table work"),
        ("table_work: all parts encoded+staged", "all parts encoded"),
        ("final_verify + Committed", "final verify + Committed"),
        ("authority", "authority"),
        ("cleanup + clear", "clear"),
        ("total", "total"),
    ]
    out = [
        "",
        "| Chain | Storage | Encode:publish | "
        + " | ".join(label for _, label in columns)
        + " | Commits/s back to back |",
        "|---|---|---|" + "---:|" * (len(columns) + 1),
    ]
    for r in IN_PROCESS:
        storage = "local" if r["storage"] == "local" else f"s3-{r['latency_ms']}ms"
        cells = []
        for key, _ in columns:
            v = r["phases_ms"].get(key)
            cells.append("-" if not v else f"{v['p50']:.0f} / {v['p99']:.0f}")
        out.append(
            f"| {r['chain']} | {storage} | {r['setting']} | " + " | ".join(cells)
            + f" | {r['back_to_back_commits_per_second']:.2f} |"
        )
    return out


def main():
    results = load(sys.argv[1:])
    lines = ["### Catch-up"] + catchup(results)
    lines += ["", "### Steady-state margin"] + windows(results) + steady(results)
    lines += ["", "### Phases (loopback S3)"]
    lines += phase_table(results, "catchup-size32m") + phase_table(results, "windows-60s")
    lines += ["", "### In-process phases, p50 / p99 (ms)"] + in_process()
    lines += ["", "### Tuning with today's flags"] + tuning(results)
    print("\n".join(lines))


if __name__ == "__main__":
    main()
