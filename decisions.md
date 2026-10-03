# Project KAIROS — Decision Log

## Stage 0: Synthetic Data Generation & Reproducibility

### 1. Persona Mix Rationale
- **Decision:** 80% Commuter, 15% Shopper, 5% Chaotic across entities and event distribution.
- **Options Considered:**
  - Uniform distribution across personas (33.3% each).
  - Skewed distribution (80/15/5).
- **Rationale:** Realistic real-world workloads are dominated by predictable, periodic access patterns (commuters cycling between home, transit, and work). A secondary cohort exhibits medium-predictability Markovian decision funnels (browsing, comparing, cart, checkout/abandon). A small 5% chaotic slice injects random attribute flips and noisy observations to rigorously test consensus timeouts, conflict resolution, and fallback branches without drowning out valid consensus signals.

### 2. Seed & Deterministic RNG Architecture
- **Decision:** Pin random seed to `42` with a single, shared `numpy.random.default_rng(seed)` instance executed in a fixed, strictly ordered call sequence.
- **Options Considered:**
  - Multi-threaded or independent per-persona RNG generators.
  - Single centralized RNG stream with fixed sequence.
- **Rationale:** Byte-level determinism across runs requires that generation order never varies across environments or executions. By sorting events chronologically (`ts_ms`) and assigning sequential monotonic indices (`seq`), downstream verification can assert identical SHA256/MD5 hashes on generated Parquet and JSON files.

### 3. Entanglement Graph Density
- **Decision:** Exactly 10% of total entities (`n_entities * 0.10`) are entangled with 1 to 3 partners each.
- **Options Considered:**
  - Dense mesh / power-law scale-free graph.
  - Sparse 10% subgraph with bounded degree (1–3).
- **Rationale:** A 10% entanglement fraction models correlated real-world entities (e.g. user session to cart, product inventory to complementary accessories) without triggering cascading consensus storms. Output keys and partner lists are strictly sorted (`sort_keys=True`) to maintain byte-idempotency.

### 4. Burst Injection Mechanics
- **Decision:** Every 1,000 events, a burst of 10 events is directed at a single target entity, counting toward the 1,000,000 total event budget.
- **Options Considered:**
  - Adding extra burst events beyond 1M budget.
  - Folding burst events into the 1M total budget.
- **Rationale:** Counting bursts toward the total event budget preserves fixed memory sizing and exact comparison window baselines across deterministic and probabilistic backends. Burst events simulate real-world thundering herds and flash updates.

### 5. Storage & Serialization
- **Decision:** PyArrow Parquet with Snappy compression and strict column schema (`int64`, `string`, `float64`), and JSON with indentation and sorted keys for entanglements.
- **Rationale:** Columnar Parquet with Snappy ensures high read throughput and compact storage, while preserving byte-identical output across consecutive runs on identical inputs.
