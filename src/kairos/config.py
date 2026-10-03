from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class KairosConfig:
    seed: int = 42
    n_events: int = 1_000_000
    n_entities: int = 1000
    span_days: int = 30
    persona_mix: Optional[Dict[str, float]] = None
    burst_every: int = 1000
    burst_size: int = 10
    entanglement_fraction: float = 0.10
    entanglement_max_partners: int = 3

    def __post_init__(self):
        if self.persona_mix is None:
            object.__setattr__(
                self,
                "persona_mix",
                {"commuter": 0.80, "shopper": 0.15, "chaotic": 0.05},
            )
