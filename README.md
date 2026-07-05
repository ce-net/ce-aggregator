# ce-aggregator

Collects building-sensor data over the CE mesh — the consumer/processor tier of the
telemetry system. **The same app runs unmodified on any node** (the Mac now, the relay/server
later): it hardcodes no host and no address. It is also the "broadcaster" of the modularity
vision — deploy a tenth sensor and it collects it with zero edits.

Part of the building-telemetry mesh (`PLAN/ce-building-sensors.md`). Providers:
[`ce-sensor-climate`](../ce-sensor-climate), [`ce-sensor-camera`](../ce-sensor-camera). Uses
the shared Python client [`ce-py`](../ce-py) (`ce.py` vendored for the script tier).

## How it works

1. **Discover.** Subscribes to `ce.sensor/announce` and learns each sensor by name (service,
   node, control topic, required capability) — never by an IP address.
2. **Subscribe with a capability.** Presents its held capability (`CE_AGG_CAP`) to each
   sensor's cap-gated control topic. The sensor verifies clearance and starts pushing.
3. **Collect.** Keeps the latest reading/frame per sensor (frame bytes are summarized to a
   size, never stored). Re-subscribes before the lease expires; prunes sensors gone quiet.
4. **Re-serve.** Answers a cap-gated `snapshot` request on `ce.agg/ctl` (verified via
   `ce-iam`, fail-closed) and announces itself on `ce.agg/announce`, so a digital-twin site
   pulls the collected state over the mesh with a capability — no HTTP, no address.

### Snapshot schema (`ce.agg.snapshot/1`)

```json
{"schema":"ce.agg.snapshot/1","aggregator":"<hex>","instance":"agg-mac","ts":...,"count":2,
 "sensors":{"ce-sensor-climate/lobby":{"kind":"climate","node":"<hex>","age_s":1.2,
   "subscribed":true,"latest":{"readings":[{"metric":"temperature","value":21.4,"unit":"C"}]}}}}
```

## Nothing hardcoded — same app on Mac and server

There is no host, address, or per-node branch anywhere. Behaviour is entirely env-driven:

| Var | Default | Meaning |
|---|---|---|
| `CE_AGG_INSTANCE` | `aggregator` | Name for this collector (e.g. `agg-mac`, `agg-relay`). |
| `CE_AGG_INTERVAL` | `5` | Seconds between keepalive/prune/announce ticks. |
| `CE_AGG_LEASE` | `60` | Subscription lease requested from sensors. |
| `CE_AGG_CAP` | – | Capability token presented to sensors (empty works when sensors run `allow`). |
| `CE_AGG_SNAPSHOT_ACTION` | `building:read` | Action a consumer must hold to read the snapshot. |
| `CE_SENSOR_AUTH` | `capiam` | How the snapshot is gated: `capiam` / `allowlist` / `allow` (dev) / `deny`. |

## Develop & test

```bash
pytest    # pure collector core: discovery, keepalive scheduling, summarize, snapshot, prune
```

## Deploy

```bash
ce app install ./ce-aggregator --on node=mac       # now
ce app install ./ce-aggregator --on node=relay     # later — identical app
```
