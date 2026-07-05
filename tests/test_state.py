"""Unit tests for AggregatorState — the pure collector core (no node, no sensors)."""

from __future__ import annotations

from hub.state import SNAPSHOT_SCHEMA, AggregatorState

CLIMATE_NODE = "aa" * 32
CAMERA_NODE = "cc" * 32


def _climate_announce(instance="lobby"):
    return {"schema": "ce.sensor.announce/1", "service": "ce-sensor-climate",
            "kind": "climate", "node": CLIMATE_NODE, "instance": instance,
            "ctl_topic": "ce.sensor/climate/ctl", "data_topic": "ce.sensor/climate/data",
            "action": "building:climate:read"}


def _camera_announce(instance="entrance"):
    return {"schema": "ce.sensor.announce/1", "service": "ce-sensor-camera",
            "kind": "camera", "node": CAMERA_NODE, "instance": instance,
            "ctl_topic": "ce.sensor/camera/ctl", "data_topic": "ce.sensor/camera/frame",
            "action_read": "building:camera:read", "action_control": "building:camera:control"}


def _reading(instance="lobby", temp=21.4):
    return {"schema": "ce.sensor.reading/1", "sensor": "ce-sensor-climate",
            "node": CLIMATE_NODE, "instance": instance, "ts": 1000.0,
            "readings": [{"metric": "temperature", "value": temp, "unit": "C"}]}


def _frame(instance="entrance", seq=3):
    return {"schema": "ce.sensor.frame/1", "sensor": "ce-sensor-camera",
            "node": CAMERA_NODE, "instance": instance, "ts": 1000.0, "seq": seq,
            "format": "png", "width": 320, "height": 240, "quality": "medium",
            "bytes_hex": "89504e47" * 100}


def test_announce_discovers_then_refreshes():
    s = AggregatorState()
    assert s.note_announce(_climate_announce(), 100.0) is True   # new
    assert s.note_announce(_climate_announce(), 105.0) is False  # refresh
    assert len(s.sensors) == 1


def test_camera_announce_uses_read_action():
    s = AggregatorState()
    s.note_announce(_camera_announce(), 100.0)
    info = s.sensors["ce-sensor-camera/entrance"]
    assert info.action == "building:camera:read"


def test_due_subscribes_flags_new_sensor_then_clears_after_marking():
    s = AggregatorState(lease=60.0, resub_margin=20.0)
    s.note_announce(_climate_announce(), 100.0)
    due = s.due_subscribes(100.0)
    assert len(due) == 1 and due[0].key == "ce-sensor-climate/lobby"
    assert due[0].ctl_topic == "ce.sensor/climate/ctl"

    s.mark_subscribed(due[0].key, 100.0 + 60.0)  # subscribed until t=160
    assert s.due_subscribes(120.0) == []           # 160-120=40 > margin 20 -> not due
    assert len(s.due_subscribes(145.0)) == 1        # 160-145=15 <= margin 20 -> due again


def test_incomplete_announce_is_not_subscribable():
    s = AggregatorState()
    s.note_announce({"schema": "ce.sensor.announce/1", "service": "x", "instance": "y"}, 100.0)
    assert s.due_subscribes(100.0) == []  # no ctl_topic/node -> can't reach it


def test_note_data_updates_latest_and_drops_frame_bytes():
    s = AggregatorState()
    s.note_announce(_camera_announce(), 100.0)
    s.note_data(_frame(seq=7), 101.0)
    latest = s.sensors["ce-sensor-camera/entrance"].latest
    assert latest["seq"] == 7 and latest["format"] == "png"
    assert "bytes_hex" not in latest       # raw bytes never stored
    assert latest["bytes"] == 400          # size recorded instead (800 hex chars / 2)


def test_data_before_announce_creates_minimal_sensor():
    s = AggregatorState()
    key = s.note_data(_reading(), 100.0)
    assert key == "ce-sensor-climate/lobby"
    assert s.sensors[key].latest["readings"][0]["value"] == 21.4


def test_snapshot_shape_and_count():
    s = AggregatorState()
    s.note_announce(_climate_announce(), 100.0)
    s.note_announce(_camera_announce(), 100.0)
    s.note_data(_reading(temp=22.0), 101.0)
    snap = s.snapshot("bb" * 32, "agg-mac", 102.0)
    assert snap["schema"] == SNAPSHOT_SCHEMA
    assert snap["count"] == 2
    assert snap["aggregator"] == "bb" * 32
    climate = snap["sensors"]["ce-sensor-climate/lobby"]
    assert climate["kind"] == "climate"
    assert climate["latest"]["readings"][0]["value"] == 22.0
    assert climate["age_s"] == 1.0
    # camera seen (announced) but no data yet
    assert snap["sensors"]["ce-sensor-camera/entrance"]["latest"] is None


def test_prune_drops_stale_sensors():
    s = AggregatorState(ttl=120.0)
    s.note_announce(_climate_announce(), 100.0)
    assert s.prune(200.0) == []                       # 100s old < ttl 120
    assert "ce-sensor-climate/lobby" in s.sensors
    dropped = s.prune(230.0)                           # 130s old > ttl 120
    assert dropped == ["ce-sensor-climate/lobby"]
    assert s.sensors == {}
