"""AggregatorState — the pure, testable core of the sensor collector.

It holds no sockets and no threads: it consumes decoded mesh messages (sensor announces
and pushed data) and tells the runtime what to do (which sensors need a subscribe request,
what the current collected snapshot is). Because it is pure, the whole collection logic is
unit-tested with plain dicts and an injectable clock — no node, no hardware, no sensors.

Nothing here is hardcoded to a specific sensor: sensors are keyed by their announced
``service/instance`` and their data is summarized generically from the shared envelope, so a
tenth sensor of any kind is collected with zero code change (the modularity vision's
"broadcaster auto-picks up new handlers").
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

ANNOUNCE_SCHEMA = "ce.sensor.announce/1"
DATA_SCHEMAS = {"ce.sensor.reading/1", "ce.sensor.frame/1"}
SNAPSHOT_SCHEMA = "ce.agg.snapshot/1"


@dataclass
class SensorInfo:
    service: str
    kind: str
    node: str
    instance: str
    ctl_topic: str
    data_topic: str
    action: str
    last_seen: float
    subscribed_until: float = 0.0
    latest: Optional[dict] = None
    latest_at: float = 0.0


@dataclass(frozen=True)
class SubscribeIntent:
    """A pending (re)subscribe the runtime should issue to a sensor's cap-gated ctl."""

    key: str
    node: str
    ctl_topic: str
    action: str


@dataclass
class AggregatorState:
    lease: float = 60.0
    # Re-subscribe once the remaining lease drops below this margin (keepalive).
    resub_margin: float = 20.0
    # Drop a sensor from the snapshot if unseen for this long.
    ttl: float = 120.0
    sensors: dict = field(default_factory=dict)

    @staticmethod
    def _key(service: str, instance: str) -> str:
        return f"{service}/{instance}"

    def note_announce(self, ann: dict, now: float) -> bool:
        """Register/refresh a sensor from its announce. Returns True if newly discovered."""
        service = ann.get("service")
        if not service:
            return False
        instance = ann.get("instance", "")
        key = self._key(service, instance)
        # A sensor may advertise a single `action` (climate) or read/control pair (camera);
        # the aggregator only reads, so it wants the read-level action.
        action = ann.get("action") or ann.get("action_read") or ""
        info = self.sensors.get(key)
        if info is None:
            self.sensors[key] = SensorInfo(
                service=service, kind=ann.get("kind", ""), node=ann.get("node", ""),
                instance=instance, ctl_topic=ann.get("ctl_topic", ""),
                data_topic=ann.get("data_topic", ""), action=action, last_seen=now)
            return True
        info.node = ann.get("node", info.node)
        info.ctl_topic = ann.get("ctl_topic", info.ctl_topic)
        info.data_topic = ann.get("data_topic", info.data_topic)
        info.kind = ann.get("kind", info.kind)
        if action:
            info.action = action
        info.last_seen = now
        return False

    def note_data(self, decoded: dict, now: float) -> Optional[str]:
        """Record the latest pushed reading/frame for its sensor. Returns the sensor key."""
        service = decoded.get("sensor")
        if not service:
            return None
        instance = decoded.get("instance", "")
        key = self._key(service, instance)
        info = self.sensors.get(key)
        if info is None:  # data arrived before we saw the announce
            info = SensorInfo(service=service, kind="", node=decoded.get("node", ""),
                              instance=instance, ctl_topic="", data_topic="",
                              action="", last_seen=now)
            self.sensors[key] = info
        info.latest = self._summarize(decoded)
        info.latest_at = now
        info.last_seen = now
        return key

    @staticmethod
    def _summarize(d: dict) -> dict:
        """Keep the useful fields; never store raw frame bytes in the collected state."""
        out = {k: d[k] for k in ("schema", "sensor", "node", "instance", "ts") if k in d}
        if "readings" in d:
            out["readings"] = d["readings"]
        for k in ("seq", "format", "width", "height", "quality"):
            if k in d:
                out[k] = d[k]
        if "bytes_hex" in d:
            out["bytes"] = len(d["bytes_hex"]) // 2  # size, not the payload
        return out

    def due_subscribes(self, now: float) -> list:
        """Sensors that need a (re)subscribe now: never subscribed, or lease near expiry."""
        out = []
        for key, info in self.sensors.items():
            if not info.ctl_topic or not info.node:
                continue  # can't reach it until we've seen a complete announce
            if info.subscribed_until - now <= self.resub_margin:
                out.append(SubscribeIntent(key, info.node, info.ctl_topic, info.action))
        return out

    def mark_subscribed(self, key: str, until: float) -> None:
        info = self.sensors.get(key)
        if info is not None:
            info.subscribed_until = until

    def prune(self, now: float) -> list:
        """Forget sensors unseen for longer than ``ttl``. Returns the dropped keys."""
        dropped = [k for k, i in self.sensors.items() if now - i.last_seen > self.ttl]
        for k in dropped:
            del self.sensors[k]
        return dropped

    def snapshot(self, node_id: str, instance: str, now: float) -> dict:
        """The collected state — what the digital twin consumes over the cap-gated snapshot."""
        sensors = {}
        for key, info in self.sensors.items():
            sensors[key] = {
                "service": info.service,
                "kind": info.kind,
                "node": info.node,
                "instance": info.instance,
                "last_seen": round(info.last_seen, 3),
                "age_s": round(now - info.latest_at, 3) if info.latest_at else None,
                "subscribed": info.subscribed_until > now,
                "latest": info.latest,
            }
        return {
            "schema": SNAPSHOT_SCHEMA,
            "aggregator": node_id,
            "instance": instance,
            "ts": round(now, 3),
            "count": len(sensors),
            "sensors": sensors,
        }
