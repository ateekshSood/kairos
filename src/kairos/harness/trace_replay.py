"""Trace replay harness and synthetic trace generator for Project KAIROS."""

import argparse
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from kairos.config import KairosConfig


OBSERVERS = (
    "api-gateway-1",
    "api-gateway-2",
    "mobile-app-ios",
    "mobile-app-android",
    "sensor-node-a",
    "batch-sync",
)

COMMUTER_CYCLE = ("home", "transit", "work", "transit")

SHOPPER_TRANSITIONS = {
    "browse": (("compare", 0.6), ("cart", 0.3), ("abandon", 0.1)),
    "compare": (("cart", 0.5), ("browse", 0.3), ("abandon", 0.2)),
    "cart": (("checkout", 0.6), ("abandon", 0.2), ("browse", 0.2)),
    "checkout": (("browse", 1.0),),
    "abandon": (("browse", 1.0),),
}

CHAOTIC_ATTRIBUTES = (
    "status",
    "telemetry",
    "mood",
    "tier",
    "preference",
    "connectivity",
)

CHAOTIC_VALUES = (
    "active",
    "idle",
    "pending",
    "error",
    "degraded",
    "unknown",
    "high",
    "low",
    "medium",
    "flapping",
)

PARQUET_SCHEMA = pa.schema(
    [
        ("ts_ms", pa.int64()),
        ("seq", pa.int64()),
        ("entity_id", pa.string()),
        ("attribute", pa.string()),
        ("value", pa.string()),
        ("observer_id", pa.string()),
        ("confidence", pa.float64()),
        ("session_id", pa.string()),
    ]
)


def generate_entanglements(
    config: KairosConfig, rng: np.random.Generator, out_path: Path
) -> Dict[str, List[str]]:
    """Generate deterministic entanglements graph for 10% of entities."""
    n_entangled = int(config.n_entities * config.entanglement_fraction)
    entangled_indices = sorted(
        rng.choice(config.n_entities, size=n_entangled, replace=False).tolist()
    )

    entanglements: Dict[str, List[str]] = {}
    for idx in entangled_indices:
        entity_id = f"entity:{idx}"
        partner_count = int(rng.integers(1, config.entanglement_max_partners + 1))
        candidate_indices = [i for i in range(config.n_entities) if i != idx]
        partner_indices = sorted(
            rng.choice(candidate_indices, size=partner_count, replace=False).tolist()
        )
        entanglements[entity_id] = sorted([f"entity:{p}" for p in partner_indices])

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(entanglements, f, indent=2, sort_keys=True)

    return entanglements


def generate_trace(
    config: KairosConfig, out_dir: str | Path
) -> Tuple[Path, Path]:
    """Generate synthetic events parquet and entanglements.json."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = out_dir / "synthetic.parquet"
    entanglements_path = out_dir / "entanglements.json"

    # Single RNG instance for strict determinism
    rng = np.random.default_rng(config.seed)

    # 1. Generate entanglements first
    generate_entanglements(config, rng, entanglements_path)

    # 2. Partition entities into personas
    mix = config.persona_mix or {"commuter": 0.80, "shopper": 0.15, "chaotic": 0.05}
    commuter_pct = mix.get("commuter", 0.80)
    shopper_pct = mix.get("shopper", 0.15)

    n_commuter = int(round(config.n_entities * commuter_pct))
    n_shopper = int(round(config.n_entities * shopper_pct))
    n_chaotic = config.n_entities - n_commuter - n_shopper

    # State trackers
    commuter_cycle_idx = [0] * n_commuter
    commuter_sess = [0] * n_commuter

    shopper_state = ["browse"] * n_shopper
    shopper_sess = [0] * n_shopper

    # Timing setup
    start_ts = 1704067200000  # 2024-01-01 00:00:00 UTC
    span_ms = config.span_days * 86400 * 1000
    avg_interval = max(1.0, span_ms / max(1, config.n_events))
    current_ts = start_ts

    def emit_event(entity_idx: int, ts: int) -> Dict[str, Any]:
        entity_id = f"entity:{entity_idx}"
        obs_idx = int(rng.integers(0, len(OBSERVERS)))
        observer_id = OBSERVERS[obs_idx]

        if entity_idx < n_commuter:
            # Commuter
            c_idx = commuter_cycle_idx[entity_idx]
            attribute = "location"
            value = COMMUTER_CYCLE[c_idx]
            commuter_cycle_idx[entity_idx] = (c_idx + 1) % len(COMMUTER_CYCLE)
            if value == "home":
                commuter_sess[entity_idx] += 1
            confidence = float(rng.uniform(0.8, 1.0))
            session_id = f"sess:{entity_id}:{commuter_sess[entity_idx]}"
        elif entity_idx < n_commuter + n_shopper:
            # Shopper
            s_idx = entity_idx - n_commuter
            attribute = "intent"
            curr_state = shopper_state[s_idx]
            value = curr_state

            # Markov transition to next state
            transitions = SHOPPER_TRANSITIONS[curr_state]
            r = float(rng.random())
            cum = 0.0
            next_state = transitions[-1][0]
            for state_name, prob in transitions:
                cum += prob
                if r < cum:
                    next_state = state_name
                    break
            shopper_state[s_idx] = next_state

            confidence = float(rng.uniform(0.5, 0.9))
            session_id = f"sess:{entity_id}:{shopper_sess[s_idx]}"
            if value in ("checkout", "abandon"):
                shopper_sess[s_idx] += 1
        else:
            # Chaotic
            attr_idx = int(rng.integers(0, len(CHAOTIC_ATTRIBUTES)))
            attribute = CHAOTIC_ATTRIBUTES[attr_idx]
            val_idx = int(rng.integers(0, len(CHAOTIC_VALUES)))
            value = CHAOTIC_VALUES[val_idx]
            confidence = float(rng.uniform(0.3, 0.8))
            rand_sess = int(rng.integers(1000, 9999))
            session_id = f"sess:{entity_id}:{rand_sess}"

        return {
            "ts_ms": ts,
            "entity_id": entity_id,
            "attribute": attribute,
            "value": value,
            "observer_id": observer_id,
            "confidence": confidence,
            "session_id": session_id,
        }

    # Generate events with periodic bursts
    events: List[Dict[str, Any]] = []
    burst_every = config.burst_every
    burst_size = config.burst_size
    events_generated = 0
    next_burst_at = burst_every

    while events_generated < config.n_events:
        # Check if next block needs a burst
        if (
            burst_every > 0
            and burst_size > 0
            and events_generated >= (next_burst_at - burst_size)
            and events_generated < next_burst_at
        ):
            # Emit burst of burst_size events to a single entity
            burst_entity_idx = int(rng.integers(0, config.n_entities))
            burst_count = min(burst_size, config.n_events - events_generated)
            for _ in range(burst_count):
                current_ts += int(rng.integers(1, 5))
                events.append(emit_event(burst_entity_idx, current_ts))
                events_generated += 1
            next_burst_at += burst_every
        else:
            # Regular event
            dt = max(1, int(rng.exponential(avg_interval)))
            current_ts += dt
            entity_idx = int(rng.integers(0, config.n_entities))
            events.append(emit_event(entity_idx, current_ts))
            events_generated += 1

    # Sort events strictly by ts_ms, then assign seq
    events.sort(key=lambda e: e["ts_ms"])
    for seq, ev in enumerate(events):
        ev["seq"] = seq

    # Build columnar data for PyArrow table
    columns = {
        "ts_ms": [e["ts_ms"] for e in events],
        "seq": [e["seq"] for e in events],
        "entity_id": [e["entity_id"] for e in events],
        "attribute": [e["attribute"] for e in events],
        "value": [e["value"] for e in events],
        "observer_id": [e["observer_id"] for e in events],
        "confidence": [e["confidence"] for e in events],
        "session_id": [e["session_id"] for e in events],
    }

    table = pa.Table.from_pydict(columns, schema=PARQUET_SCHEMA)
    pq.write_table(table, parquet_path, compression="snappy")

    return parquet_path, entanglements_path


def print_stats(trace_path: str | Path) -> None:
    """Read trace Parquet file and print comprehensive summary statistics."""
    trace_path = Path(trace_path)
    if not trace_path.exists():
        print(f"Error: Trace file not found at {trace_path}")
        return

    table = pq.read_table(trace_path)
    df = table.to_pandas()

    total_events = len(df)
    unique_entities = df["entity_id"].nunique()
    unique_observers = df["observer_id"].nunique()
    unique_attributes = df["attribute"].nunique()

    min_ts = int(df["ts_ms"].min())
    max_ts = int(df["ts_ms"].max())
    span_seconds = (max_ts - min_ts) / 1000.0
    span_days = span_seconds / 86400.0
    event_rate = total_events / max(0.001, span_seconds)

    commuter_events = int((df["attribute"] == "location").sum())
    shopper_events = int((df["attribute"] == "intent").sum())
    chaotic_events = total_events - commuter_events - shopper_events

    # Fast candidate-masked burst analysis: find 10 consecutive events for same entity with ts span <= 100ms
    burst_count = 0
    entities = df["entity_id"].values
    ts = df["ts_ms"].values
    n = len(entities)
    if n >= 10:
        candidate_mask = (entities[:-9] == entities[9:]) & ((ts[9:] - ts[:-9]) <= 100)
        candidate_indices = np.where(candidate_mask)[0]
        last_end = -1
        for idx in candidate_indices:
            if idx >= last_end:
                if np.all(entities[idx : idx + 10] == entities[idx]):
                    burst_count += 1
                    last_end = idx + 10

    print("==================================================")
    print("           PROJECT KAIROS — TRACE STATS           ")
    print("==================================================")
    print(f"Trace File:          {trace_path}")
    print(f"Total Events:        {total_events:,}")
    print(f"Unique Entities:     {unique_entities:,}")
    print(f"Unique Observers:    {unique_observers:,}")
    print(f"Unique Attributes:   {unique_attributes:,}")
    print(f"Time Span:           {span_days:.2f} days ({span_seconds:,.0f} s)")
    print(f"Event Rate:          {event_rate:.2f} events/sec")
    print("--------------------------------------------------")
    print("Persona Breakdown (Events):")
    print(
        f"  - Commuter:        {commuter_events:,} ({commuter_events / total_events * 100:.1f}%)"
    )
    print(
        f"  - Shopper:         {shopper_events:,} ({shopper_events / total_events * 100:.1f}%)"
    )
    print(
        f"  - Chaotic:         {chaotic_events:,} ({chaotic_events / total_events * 100:.1f}%)"
    )
    print("--------------------------------------------------")
    print(f"Detected 10x Bursts: {burst_count:,}")
    print("Confidence Stats:")
    print(f"  - Min:             {df['confidence'].min():.4f}")
    print(f"  - Mean:            {df['confidence'].mean():.4f}")
    print(f"  - Max:             {df['confidence'].max():.4f}")
    print("==================================================")


def main():
    parser = argparse.ArgumentParser(description="KAIROS trace generator and inspector")
    parser.add_argument("--generate", action="store_true", help="Generate synthetic trace")
    parser.add_argument("--stats", action="store_true", help="Print trace statistics")
    parser.add_argument("--out", type=str, default="data/processed/", help="Output directory")
    parser.add_argument(
        "--trace",
        type=str,
        default="data/processed/synthetic.parquet",
        help="Path to trace parquet file",
    )
    parser.add_argument("--seed", type=int, default=42, help="RNG seed")
    parser.add_argument("--events", type=int, default=None, help="Total events override")
    parser.add_argument("--entities", type=int, default=None, help="Total entities override")

    args = parser.parse_args()

    if args.generate:
        config_kwargs: Dict[str, Any] = {"seed": args.seed}
        if args.events is not None:
            config_kwargs["n_events"] = args.events
        if args.entities is not None:
            config_kwargs["n_entities"] = args.entities

        config = KairosConfig(**config_kwargs)
        parquet_path, entanglements_path = generate_trace(config, args.out)
        print(f"Generated trace at {parquet_path}")
        print(f"Generated entanglements at {entanglements_path}")

    if args.stats:
        print_stats(args.trace)


if __name__ == "__main__":
    main()
