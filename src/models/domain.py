from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Annotated

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

TraceId = Annotated[str, Field(pattern=r"^[A-Za-z0-9_.-]+$", max_length=160)]
ActorURN = Annotated[str, Field(pattern=r"^urn:actor:[^:\s]+$")]
SystemURN = Annotated[str, Field(pattern=r"^urn:system:[^:\s]+$")]
ResourceURN = Annotated[str, Field(pattern=r"^urn:resource:[^:\s]+:[^:\s]+:[^:\s]+$")]


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    trace_id: TraceId
    timestamp: AwareDatetime
    actor_urn: ActorURN
    system_urn: SystemURN
    action: Annotated[str, Field(min_length=1, max_length=250)]
    resource_urn: ResourceURN
    duration_ms: Annotated[int, Field(ge=0)]

    @field_validator("timestamp")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        if value.microsecond % 1000:
            raise ValueError("timestamp must have at most millisecond precision")
        return value.astimezone(UTC)

    def stamp(self) -> str:
        return self.timestamp.isoformat(timespec="milliseconds")

    def event_key(self) -> str:
        fields = (self.trace_id, self.stamp(), self.actor_urn, self.system_urn,
                  self.action, self.resource_urn, self.duration_ms)
        encoded = json.dumps(fields, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class Step(Event):
    step_index: int
    urn: str


class Summary(BaseModel):
    received: int
    inserted: int
    duplicates: int


class Loop(BaseModel):
    trace_id: str
    action: str
    step_indices: list[int]


class Bottleneck(BaseModel):
    trace_id: str
    from_index: int
    to_index: int
    from_action: str
    to_action: str
    elapsed_ms: int
    threshold_ms: float
