# Resilience Demo — Kafka AI Agent (Final Project)

Three failure scenarios with expected behavior and recovery steps.

All commands assume the repository root as the working directory with Kafka
running via `infra/docker-compose.yml`.

---

## Scenario 1 — Worker Crash

**What breaks:** A tool worker process is killed while a plan is in-flight.

**Why it recovers:** Kafka retains unacknowledged messages at the last committed
offset. When the worker restarts it rejoins the consumer group and replays any
unprocessed `ToolInvocationRequested` events automatically.

### Steps

```bash
# 1. Start all services
bun run start
open http://localhost:3001   # or bun run web:dev for hot-reload

# 2. Send a query that routes to the math worker
#    Type in the UI:
(12 + 8) * 5

# 3. Confirm the answer arrives, then kill the math worker
pkill -f "mathApp.ts"

# 4. Send another math query while the worker is down
#    Type in the UI:
(99 - 9) / 3
#    The orchestrator dispatches the ToolInvocationRequested to Kafka.
#    The plan is pending — no answer arrives yet.

# 5. Restart the math worker
bun src/node/apps/mathApp.ts >> scripts/logs/final-project-services/math.log 2>&1 &

#    The worker picks up the pending message from the last committed offset,
#    computes the result, and emits ToolInvocationResulted.
#    The orchestrator completes the plan and the UI receives the answer.
```

### Expected log output (orchestrator.log)

```
[orchestrator] conv-xxx dispatching tool="math"
[orchestrator] conv-xxx waiting for ToolInvocationResulted ...
# (worker is killed here — silence for a period)
[orchestrator] conv-xxx step 1/1 completed tool="math" toolLatency=Xms
[orchestrator] conv-xxx plan completed, results=1
```

### Expected outcome

The UI receives the correct math answer after the worker restarts. No manual
Kafka intervention is required. In-flight plans dispatched before the crash
resume automatically.

---

## Scenario 2 — Orchestrator Crash

**What breaks:** The orchestrator process is killed and restarted.

**Why it recovers:** The orchestrator persists plan state to LevelDB via
`shared/state/planStore.ts`, so a crash does not lose it. Recovery is
**event-driven, not active**: on startup `initializeStore()` only opens the
database — nothing scans it for pending work. A plan resumes when the next event
for its `conversationId` arrives and `getPlan()` finds the saved state.

**Known gap:** if the process died after `savePlan()` but before its
`ToolInvocationRequested` was published, no result will ever arrive and nothing
re-dispatches the step. That plan stays in LevelDB indefinitely — with
`status: "pending"` if it was the first step, since `status` is only set to
`"running"` after the dispatch succeeds, or `"running"` if a later step was
already in flight. Closing this would require a startup pass over the store that
re-dispatches every plan in either state.

### Steps

```bash
# 1. Start all services
bun run start
open http://localhost:3001   # or bun run web:dev for hot-reload

# 2. Verify end-to-end works
#    Type in the UI:
weather in jerusalem
#    Confirm the answer arrives.

# 3. Kill the orchestrator
pkill -f "orchestrator.ts"

# 4. Restart the orchestrator
bun src/node/orchestration/orchestrator.ts \
  >> scripts/logs/final-project-services/orchestrator.log 2>&1 &

#    On startup the orchestrator only opens LevelDB — it does not scan it.
#    Saved state is picked up when the next event for that conversation arrives.

# 5. Send a new query
#    Type in the UI:
convert 50 eur to usd
#    The restarted orchestrator picks up the PlanGenerated event from Kafka,
#    stores state in LevelDB, dispatches the tool, and completes the plan.
```

### Expected log output (orchestrator.log)

```
[orchestrator] PlanStore initialized
[orchestrator] OrchestratorService started.
[orchestrator] conversationId=conv-yyy plan received steps=[exchange]
[orchestrator] conversationId=conv-yyy dispatching tool="exchange"
[orchestrator] conversationId=conv-yyy step 1/1 completed tool="exchange"
[orchestrator] conversationId=conv-yyy plan completed, results=1
```

### Expected outcome

New requests complete end-to-end after the restart. An in-flight plan whose
state was persisted before the crash resumes as soon as its next
`ToolInvocationResulted` arrives — provided the tool request had already been
published before the crash. If it had not, the plan does not resume on its own.

---

## Scenario 3 — Duplicate Events

**What breaks:** A `ToolInvocationRequested` or `ToolInvocationResulted` event
is delivered more than once (e.g., consumer offset not committed, Kafka retry,
or manual re-publish via CLI).

**Why it is handled idempotently:** Two independent guards prevent duplicate
processing.

### Guard 1 — Tool workers filter by `toolName`

Each worker checks the `tool` field of every `ToolInvocationRequested` event
before processing. Events intended for another worker are silently skipped.

```typescript
// src/node/apps/mathApp.ts
if (req.payload.toolName !== "math") return;

// src/node/apps/weatherApp.ts
if (req.payload.toolName !== "weather") return;
```

A duplicate request broadcast to all workers is processed only by the correct
worker. All others discard it.

### Guard 2 — Orchestrator drops stale `conversationId` results

When a `ToolInvocationResulted` arrives the orchestrator looks up the
`conversationId` in its state store. If the state no longer exists (plan already
completed and state removed) the event is discarded.

```typescript
// src/node/orchestration/orchestrator.ts
if (!state) {
  console.warn(`[orchestrator] unknown conversationId=${conversationId}, skipping.`);
  return;
}
```

### Steps to reproduce manually

```bash
# After a plan has already completed, re-publish the ToolInvocationResulted
# event using the Kafka CLI.

# 1. Read recent messages from conversation-events
docker exec -it kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic conversation-events \
  --from-beginning \
  --max-messages 30

# 2. Copy a ToolInvocationResulted JSON, then re-publish it
docker exec -it kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic conversation-events
# Paste the JSON and press Enter.
```

### Expected log output (orchestrator.log)

```
[orchestrator] conv-zzz plan completed, results=1
[Benchmark]    conv-zzz synthesizerLatency=Xms
[orchestrator] unknown conversationId=conv-zzz, skipping.
```

### Expected outcome

The UI receives exactly one answer. The duplicate event is silently dropped by
the orchestrator. No duplicate answer is displayed.

**Scope of this guard:** it relies on `deletePlan()` having removed the state at
completion, so it covers duplicates that arrive *after* a plan finishes. There is
no deduplication key, so a duplicate `ToolInvocationResulted` arriving *mid-plan*
would be appended twice and advance `stepIndex` by two, skipping a step. The
delivery model is at-least-once with idempotent completion guards, not
end-to-end exactly-once.

---

## Summary

| Test                | Mechanism                              | Recovery    |
|---------------------|----------------------------------------|-------------|
| Worker crash        | Kafka offset replay on restart         | Automatic   |
| Orchestrator crash  | LevelDB state survives; resumes on next event | Event-driven |
| Duplicate events    | `tool` name filter + conversationId guard | Silent drop |
