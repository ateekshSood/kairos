"""Tests for Stage 0 — Synthetic Data Generation & Reproducibility."""

import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from pydantic import ValidationError
import pytest

from kairos.config import KairosConfig
from kairos.harness.trace_replay import (
    generate_trace,
    print_stats,
)
from kairos.models import ObservationEvent


@pytest.fixture(scope="session")
def test_config():
    """Fast test configuration: 20k events, 200 entities."""
    return KairosConfig(
        seed=42,
        n_events=20_000,
        n_entities=200,
        span_days=30,
        burst_every=1000,
        burst_size=10,
        entanglement_fraction=0.10,
        entanglement_max_partners=3,
    )


@pytest.fixture(scope="session")
def generated_trace(tmp_path_factory, test_config):
    """Session-scoped trace generation to keep tests fast."""
    out_dir = tmp_path_factory.mktemp("stage0_data")
    parquet_path, entanglements_path = generate_trace(test_config, out_dir)
    table = pq.read_table(parquet_path)
    df = table.to_pandas()
    with open(entanglements_path, "r", encoding="utf-8") as f:
        entanglements = json.load(f)
    return {
        "out_dir": out_dir,
        "parquet_path": parquet_path,
        "entanglements_path": entanglements_path,
        "table": table,
        "df": df,
        "entanglements": entanglements,
    }


def test_1_byte_idempotency(tmp_path, test_config):
    """Test 1: Byte-idempotency across two independent runs with identical seed."""
    dir1 = tmp_path / "run1"
    dir2 = tmp_path / "run2"

    p1, e1 = generate_trace(test_config, dir1)
    p2, e2 = generate_trace(test_config, dir2)

    with open(p1, "rb") as f1, open(p2, "rb") as f2:
        h1 = hashlib.sha256(f1.read()).hexdigest()
        h2 = hashlib.sha256(f2.read()).hexdigest()
    assert h1 == h2, f"Parquet sha256 mismatch: {h1} != {h2}"

    with open(e1, "rb") as f1, open(e2, "rb") as f2:
        ej1 = hashlib.sha256(f1.read()).hexdigest()
        ej2 = hashlib.sha256(f2.read()).hexdigest()
    assert ej1 == ej2, f"Entanglements sha256 mismatch: {ej1} != {ej2}"


def test_2_seed_sensitivity(tmp_path):
    """Test 2: Changing seed alters output parquet and entanglements."""
    cfg42 = KairosConfig(seed=42, n_events=2000, n_entities=50)
    cfg43 = KairosConfig(seed=43, n_events=2000, n_entities=50)

    p1, e1 = generate_trace(cfg42, tmp_path / "s42")
    p2, e2 = generate_trace(cfg43, tmp_path / "s43")

    with open(p1, "rb") as f1, open(p2, "rb") as f2:
        assert hashlib.sha256(f1.read()).hexdigest() != hashlib.sha256(f2.read()).hexdigest()

    with open(e1, "rb") as f1, open(e2, "rb") as f2:
        assert hashlib.sha256(f1.read()).hexdigest() != hashlib.sha256(f2.read()).hexdigest()


def test_3_schema_and_dtypes(generated_trace):
    """Test 3: Parquet schema, column names, dtypes, and non-null constraints."""
    table = generated_trace["table"]
    df = generated_trace["df"]

    expected_cols = [
        "ts_ms",
        "seq",
        "entity_id",
        "attribute",
        "value",
        "observer_id",
        "confidence",
        "session_id",
    ]
    assert table.column_names == expected_cols

    schema = table.schema
    assert schema.field("ts_ms").type.equals("int64") or str(schema.field("ts_ms").type) == "int64"
    assert schema.field("seq").type.equals("int64") or str(schema.field("seq").type) == "int64"
    assert str(schema.field("entity_id").type) == "string"
    assert str(schema.field("attribute").type) == "string"
    assert str(schema.field("value").type) == "string"
    assert str(schema.field("observer_id").type) == "string"
    assert str(schema.field("confidence").type) in ("double", "float64")
    assert str(schema.field("session_id").type) == "string"

    # Non-null guarantee
    assert df.isnull().sum().sum() == 0


def test_4_seq_ordering(generated_trace):
    """Test 4: seq ordering is strictly monotonic and matches ts_ms order."""
    df = generated_trace["df"]

    # seq is strictly 0, 1, ..., N-1
    expected_seq = list(range(len(df)))
    assert df["seq"].tolist() == expected_seq

    # ts_ms is monotonically non-decreasing
    diffs = df["ts_ms"].diff().dropna()
    assert (diffs >= 0).all()


def test_5_entity_and_event_counts(generated_trace, test_config):
    """Test 5: Total event count and unique entity count match configuration."""
    df = generated_trace["df"]
    assert len(df) == test_config.n_events
    assert df["entity_id"].nunique() == test_config.n_entities


def test_6_persona_mix_tolerance(generated_trace, test_config):
    """Test 6: Persona distribution matches 80/15/5 within statistical tolerance."""
    df = generated_trace["df"]
    total = len(df)

    commuter_events = (df["attribute"] == "location").sum()
    shopper_events = (df["attribute"] == "intent").sum()
    chaotic_events = total - commuter_events - shopper_events

    commuter_share = commuter_events / total
    shopper_share = shopper_events / total
    chaotic_share = chaotic_events / total

    assert 0.75 <= commuter_share <= 0.85, f"Commuter share {commuter_share} out of tolerance"
    assert 0.12 <= shopper_share <= 0.18, f"Shopper share {shopper_share} out of tolerance"
    assert 0.02 <= chaotic_share <= 0.08, f"Chaotic share {chaotic_share} out of tolerance"


def test_7_burst_detection(generated_trace, test_config):
    """Test 7: Detection of 10-event bursts directed at a single entity."""
    df = generated_trace["df"]
    entities = df["entity_id"].values
    ts = df["ts_ms"].values

    expected_bursts = test_config.n_events // test_config.burst_every
    detected_bursts = 0

    i = 0
    while i <= len(entities) - test_config.burst_size:
        window_entities = entities[i : i + test_config.burst_size]
        window_span = ts[i + test_config.burst_size - 1] - ts[i]
        if len(set(window_entities)) == 1 and window_span <= 100:
            detected_bursts += 1
            i += test_config.burst_size
        else:
            i += 1

    assert detected_bursts == expected_bursts, (
        f"Expected {expected_bursts} bursts, detected {detected_bursts}"
    )


def test_8_commuter_transition_validity(generated_trace):
    """Test 8: Commuter entity location transitions follow cycle >= 95% validity."""
    df = generated_trace["df"]
    commuter_df = df[df["attribute"] == "location"]

    valid_transitions = {
        "home": {"transit"},
        "transit": {"work", "home"},
        "work": {"transit"},
    }

    total_transitions = 0
    valid_count = 0

    for _, group in commuter_df.groupby("entity_id"):
        values = group["value"].tolist()
        for v1, v2 in zip(values[:-1], values[1:]):
            total_transitions += 1
            if v2 in valid_transitions.get(v1, set()):
                valid_count += 1

    assert total_transitions > 0
    validity = valid_count / total_transitions
    assert validity >= 0.95, f"Commuter transition validity was {validity:.3f} (< 0.95)"


def test_9_chaotic_entropy(generated_trace):
    """Test 9: Chaotic persona exhibits significantly higher value entropy than structured personas."""
    df = generated_trace["df"]

    def shannon_entropy(series: pd.Series) -> float:
        probs = series.value_counts(normalize=True).values
        return -sum(p * math.log2(p) for p in probs if p > 0)

    commuter_entropy = shannon_entropy(df[df["attribute"] == "location"]["value"])
    chaotic_entropy = shannon_entropy(
        df[~df["attribute"].isin(["location", "intent"])]["value"]
    )

    # Chaotic value entropy must be noticeably higher than commuter cycle
    assert chaotic_entropy > commuter_entropy + 0.8, (
        f"Chaotic entropy ({chaotic_entropy:.2f}) not sufficiently greater than "
        f"commuter ({commuter_entropy:.2f})"
    )


def test_10_per_persona_confidence_bounds(generated_trace):
    """Test 10: Per-persona confidence scores fall within specified uniform bounds."""
    df = generated_trace["df"]

    commuter_conf = df[df["attribute"] == "location"]["confidence"]
    shopper_conf = df[df["attribute"] == "intent"]["confidence"]
    chaotic_conf = df[~df["attribute"].isin(["location", "intent"])]["confidence"]

    # Commuter: U(0.8, 1.0)
    assert commuter_conf.min() >= 0.80 - 1e-6
    assert commuter_conf.max() <= 1.00 + 1e-6

    # Shopper: U(0.5, 0.9)
    assert shopper_conf.min() >= 0.50 - 1e-6
    assert shopper_conf.max() <= 0.90 + 1e-6

    # Chaotic: U(0.3, 0.8)
    assert chaotic_conf.min() >= 0.30 - 1e-6
    assert chaotic_conf.max() <= 0.80 + 1e-6


def test_11_entanglement_structure_determinism(generated_trace, test_config):
    """Test 11: Entanglement graph structure, partner counts, and byte determinism."""
    entanglements = generated_trace["entanglements"]
    expected_count = int(test_config.n_entities * test_config.entanglement_fraction)

    assert len(entanglements) == expected_count

    # Keys are sorted
    keys = list(entanglements.keys())
    assert keys == sorted(keys)

    for entity_id, partners in entanglements.items():
        assert 1 <= len(partners) <= test_config.entanglement_max_partners
        assert entity_id not in partners
        assert partners == sorted(partners)


def test_12_stats_command_output(generated_trace, capsys):
    """Test 12: trace_replay --stats execution and output formatting."""
    print_stats(generated_trace["parquet_path"])
    captured = capsys.readouterr().out

    assert "PROJECT KAIROS — TRACE STATS" in captured
    assert "Total Events:        20,000" in captured
    assert "Unique Entities:     200" in captured
    assert "Commuter:" in captured
    assert "Shopper:" in captured
    assert "Chaotic:" in captured
    assert "Detected 10x Bursts: 20" in captured
    assert "Confidence Stats:" in captured


def test_13_pydantic_validation(generated_trace):
    """Test 13: Pydantic ObservationEvent model parses valid rows and rejects invalid payloads."""
    df = generated_trace["df"]
    sample_row = df.iloc[0].to_dict()

    # Valid event parses successfully
    event = ObservationEvent(**sample_row)
    assert event.entity_id == sample_row["entity_id"]
    assert event.confidence == sample_row["confidence"]

    # Invalid confidence > 1.0
    bad_conf = sample_row.copy()
    bad_conf["confidence"] = 1.5
    with pytest.raises(ValidationError):
        ObservationEvent(**bad_conf)

    # Invalid confidence < 0.0
    bad_conf["confidence"] = -0.1
    with pytest.raises(ValidationError):
        ObservationEvent(**bad_conf)

    # Missing required field
    missing_attr = sample_row.copy()
    del missing_attr["attribute"]
    with pytest.raises(ValidationError):
        ObservationEvent(**missing_attr)
