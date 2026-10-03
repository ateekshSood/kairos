from typing import Dict, Optional
from pydantic import BaseModel, Field


class ObservationEvent(BaseModel):
    ts_ms: int
    seq: int
    entity_id: str
    attribute: str
    value: str
    observer_id: str
    confidence: float = Field(ge=0.0, le=1.0)
    session_id: str


class CollapsedState(BaseModel):
    entity_id: str
    attribute: str
    value: str
    certainty: float = Field(ge=0.0, le=1.0)
    collapsed_at: int
    fallback_reason: Optional[str] = None


class ProbabilityCloud(BaseModel):
    entity_id: str
    attribute: str
    hypotheses: Dict[str, float]
    entropy: float
    last_updated_ms: int
