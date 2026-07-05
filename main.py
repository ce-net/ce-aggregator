#!/usr/bin/env python3
"""ce-aggregator runtime — collects sensor data over the mesh. Nothing hardcoded.

The SAME app runs unmodified on the Mac and on the relay/server: it never names a host or
an address. It:
- subscribes to `ce.sensor/announce` and discovers sensors by name;
- presents a held capability to each sensor's cap-gated control topic to subscribe;
- collects the latest reading/frame pushed by each sensor (directed sends);
- re-serves the collected state on a cap-gated `snapshot` (mesh + capability, no HTTP) so a
  digital-twin site can pull it; and announces itself so the twin can find it by name.

Config (all optional, env-driven — no flags, no addresses):
- `CE_AGG_INSTANCE`        a name for this collector (default `aggregator`).
- `CE_AGG_INTERVAL`        seconds between keepalive/prune/announce ticks (default 5).
- `CE_AGG_LEASE`           subscription lease seconds requested from sensors (default 60).
- `CE_AGG_CAP`             the capability token this collector PRESENTS to sensors (default "").
- `CE_AGG_SNAPSHOT_ACTION` action a consumer must hold to read the snapshot (default `building:read`).
- `CE_SENSOR_AUTH`         how the snapshot is gated: `capiam` (default) | `allowlist` | `allow` | `deny`.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time

import ce

from capauth import authorizer_from_env
from hub.state import ANNOUNCE_SCHEMA, DATA_SCHEMAS, AggregatorState

SENSOR_ANNOUNCE = "ce.sensor/announce"
AGG_CTL = "ce.agg/ctl"
AGG_ANNOUNCE = "ce.agg/announce"

log = logging.getLogger("ce-aggregator")


def main() -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    client = ce.connect().wait_ready()
    node_id = client.node_id
    instance = os.environ.get("CE_AGG_INSTANCE", "aggregator")
    interval = float(os.environ.get("CE_AGG_INTERVAL", "5"))
    lease = float(os.environ.get("CE_AGG_LEASE", "60"))
    present_cap = os.environ.get("CE_AGG_CAP", "")
    snapshot_action = os.environ.get("CE_AGG_SNAPSHOT_ACTION", "building:read")
    authorizer = authorizer_from_env()

    state = AggregatorState(lease=lease)
    log.info("ce-aggregator (%s) up on node %s; collecting sensor data over the mesh",
             instance, node_id[:16])

    def now() -> float:
        return time.time()

    def keepalive_loop() -> None:
        while True:
            for intent in state.due_subscribes(now()):
                payload = json.dumps({"op": "subscribe", "cap": present_cap}).encode("utf-8")
                try:
                    client.request(intent.node, intent.ctl_topic, payload, timeout_ms=8000)
                    state.mark_subscribed(intent.key, now() + lease)
                    log.info("subscribed to %s on %s", intent.key, intent.node[:12])
                except ce.CeError as e:
                    log.warning("subscribe to %s failed: %s", intent.key, e)
            state.prune(now())
            try:
                client.publish(AGG_ANNOUNCE, json.dumps({
                    "schema": "ce.agg.announce/1", "service": "ce-aggregator",
                    "node": node_id, "instance": instance, "ctl_topic": AGG_CTL,
                    "action": snapshot_action,
                }, separators=(",", ":")).encode("utf-8"))
            except ce.CeError as e:
                log.warning("self-announce failed: %s", e)
            snap = state.snapshot(node_id, instance, now())
            log.info("collecting %d sensor(s): %s", snap["count"],
                     ", ".join(sorted(snap["sensors"].keys())) or "(none yet)")
            time.sleep(interval)

    threading.Thread(target=keepalive_loop, name="keepalive", daemon=True).start()

    # Main loop: consume the mesh. Announces discover sensors; directed sends carry data;
    # directed requests on AGG_CTL are cap-gated snapshot pulls.
    for m in client.messages(subscribe=[SENSOR_ANNOUNCE, AGG_CTL]):
        try:
            obj = m.json()
        except (ValueError, UnicodeDecodeError):
            obj = None

        if isinstance(obj, dict) and obj.get("schema") == ANNOUNCE_SCHEMA:
            if state.note_announce(obj, now()):
                log.info("discovered sensor %s/%s (%s) on %s",
                         obj.get("service"), obj.get("instance"), obj.get("kind"),
                         (obj.get("node") or "")[:12])
        elif isinstance(obj, dict) and obj.get("schema") in DATA_SCHEMAS:
            state.note_data(obj, now())
        elif m.topic == AGG_CTL and m.reply_token is not None:
            _handle_snapshot(client, state, authorizer, node_id, instance,
                             snapshot_action, m, now())
    return 0


def _handle_snapshot(client, state, authorizer, node_id, instance, action, msg, now) -> None:
    cap = msg.json().get("cap", "") if _is_json_obj(msg) else ""
    if not authorizer.authorize(cap, action, msg.sender, node_id):
        client.reply(msg.reply_token,
                     json.dumps({"error": f"unauthorized: need {action}"}).encode("utf-8"))
        return
    client.reply(msg.reply_token,
                 json.dumps(state.snapshot(node_id, instance, now)).encode("utf-8"))


def _is_json_obj(msg) -> bool:
    try:
        return isinstance(msg.json(), dict)
    except (ValueError, UnicodeDecodeError):
        return False


if __name__ == "__main__":
    raise SystemExit(main())
