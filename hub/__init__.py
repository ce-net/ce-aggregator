"""ce-aggregator — a nothing-hardcoded sensor collector ceapp for the CE mesh.

The same app runs unmodified on the Mac and the relay/server: it discovers sensors by
announce, subscribes to each by presenting a capability, collects the latest data, and
re-serves it on a cap-gated snapshot for the digital twin. No addresses, no per-host code.
"""

from .state import (
    ANNOUNCE_SCHEMA,
    DATA_SCHEMAS,
    SNAPSHOT_SCHEMA,
    AggregatorState,
    SensorInfo,
    SubscribeIntent,
)

__all__ = [
    "AggregatorState",
    "SensorInfo",
    "SubscribeIntent",
    "ANNOUNCE_SCHEMA",
    "DATA_SCHEMAS",
    "SNAPSHOT_SCHEMA",
]
