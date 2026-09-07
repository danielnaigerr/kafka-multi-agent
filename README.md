# Distributed Multi-Agent Backend System

An event-driven multi-agent system built on Apache Kafka. A user question is decomposed by an LLM into an ordered tool plan, executed step by step across independent workers that never call each other directly, and synthesized back into a single answer. Every state transition is an immutable Kafka event.

The point of the architecture is decoupling slow LLM calls from the request path: the web server publishes a command and returns immediately, and the answer arrives later over a WebSocket. Ten processes coordinate through four topics and eight event contracts — no service holds a reference to any other.

---

## Runs at zero API cost

`LLM_PROVIDER=auto` — the default — tries a **local Ollama model first** and falls back to OpenAI only when Ollama is unreachable. With Ollama running, no request leaves the machine and no paid API call is made.

| `LLM_PROVIDER` | Behaviour |
|---|---|
| `auto` *(default)* | Local Ollama first; automatic fallback to `gpt-4o-mini` on any failure |
| `ollama` | Local only — fails loudly if Ollama is down |
| `openai` | Cloud only |

The router adds a third tier: if **both** providers fail, it falls back to a deterministic regex planner and still produces a usable plan. The system degrades rather than breaks.

`bun run start` detects a running Ollama instance and pulls the configured model automatically; if Ollama is not reachable it logs the fact and continues.

### Model capability — measured

Running locally is free, but the default `llama3.2:1b` is not equally suited to both LLM calls in the pipeline. Measured against this repository:

| Call | Prompt size | `llama3.2:1b` (CPU) |
|---|---|---|
| Short structured request | ~10 tokens | **611 ms**, correct JSON |
| `routerPlanPrompt` | ~1,900 tokens (~20 few-shot examples) | **111.6 s**, degenerate repetition |

The router prompt is large enough to push a 1B model into a repetition loop. Its output stayed *structurally* valid — duplicate JSON keys parse, last one wins — so it satisfied the router's shape validation and yielded a wrong plan instead of triggering the regex fallback.

Practical guidance: `llama3.2:1b` is fine for answer synthesis, whose prompt is far shorter. For a fully local run that also routes well, point `OLLAMA_MODEL` at a larger model. With `gpt-4o-mini` the router averages ~1,113 ms — see [`docs/benchmark.md`](docs/benchmark.md).

---

## Request flow

```
USER
 │  question over WebSocket
 ▼
webServer.ts ─────────────► user-commands            UserQueryReceived
 │
 ▼
routerService.ts ─────────► conversation-events      PlanGenerated
 │   LLM produces a JSON tool plan, validated against a closed tool list.
 │   Falls back to a regex planner if the LLM is unavailable or returns junk.
 ▼
orchestrator.ts ──────────► tool-invocation-requests ToolInvocationRequested
 │   State machine over LevelDB, keyed by conversationId.
 │   Dispatches ONE step at a time, resolving {{step_N.result}} placeholders
 │   from results accumulated so far.
 ▼
tool workers  (five independent consumer groups, each filtering by toolName)
 │   mathApp · weatherApp · exchangeApp · generalChatApp · rag_retriever.py
 │
 └──────────────────────► conversation-events        ToolInvocationResulted
 ▼
orchestrator.ts (resumes)
 │   more steps  → dispatch the next one
 │   all done    → PlanCompleted
 │   tool error  → PlanFailed + dead-letter-queue
 ▼
aggregator.ts ────────────► user-commands            SynthesizeFinalAnswerRequested
 │   CQRS bridge: turns an event (fact) back into a command (intent).
 ▼
answerSynthesizer.ts ─────► conversation-events      FinalAnswerSynthesized
 │   One LLM call over every tool result plus session history.
 ▼
webServer.ts ─────────────► USER
```

Every message is **keyed by `conversationId`**. That guarantees ordering within a conversation while allowing independent conversations to be processed in parallel across the three partitions.

---

## Kafka topics

4 topics × 3 partitions, single broker, KRaft mode (no ZooKeeper).

| Topic | Role | Carries |
|---|---|---|
| `user-commands` | Commands | `UserQueryReceived`, `SynthesizeFinalAnswerRequested` |
| `conversation-events` | Events (append-only facts) | `PlanGenerated`, `ToolInvocationResulted`, `PlanCompleted`, `PlanFailed`, `FinalAnswerSynthesized` |
| `tool-invocation-requests` | Commands | `ToolInvocationRequested` |
| `dead-letter-queue` | Failures | Unrecoverable errors from the router and orchestrator |

Splitting commands from events keeps each service to one narrow job: producers never learn who consumes their output, and consumers never mutate shared state.

---

## Services

| Service | File | Consumer group | Stateful |
|---|---|---|---|
| WebServer | `src/node/core/webServer.ts` | `ui-web-final-answer` | in-memory (sockets) |
| RouterService | `src/node/core/routerService.ts` | `router-plan-service` | no |
| Orchestrator | `src/node/orchestration/orchestrator.ts` | `orchestrator-service` | **LevelDB** |
| Aggregator | `src/node/orchestration/aggregator.ts` | `aggregator-service` | no |
| AnswerSynthesizer | `src/node/orchestration/answerSynthesizer.ts` | `answer-synthesizer` | in-memory cache |
| mathApp | `src/node/apps/mathApp.ts` | `math-tool-worker` | no |
| weatherApp | `src/node/apps/weatherApp.ts` | `weather-tool-worker` | no |
| exchangeApp | `src/node/apps/exchangeApp.ts` | `exchange-tool-worker` | no |
| generalChatApp | `src/node/apps/generalChatApp.ts` | `chat-tool-worker` | no |
| RAG Retriever | `src/python/rag/rag_retriever.py` | `rag-tool-worker` | ChromaDB (read-only) |

---

## Retrieval (RAG)

`index_kb.py` splits each product document into chunks of at most **400 characters**, preferring paragraph boundaries and falling back to sentence boundaries, then embeds them with **`all-MiniLM-L6-v2`** into a persistent ChromaDB collection using cosine distance.

`rag_retriever.py` is a Kafka consumer, not an HTTP service. It embeds the incoming query with the same model, returns the **top 3** chunks with relevance scores, and publishes the joined text as a `ToolInvocationResulted` event. Embedding runs locally, so retrieval costs nothing and adds roughly 65 ms.

---

## Resilience

| Failure | Mechanism | Recovery |
|---|---|---|
| Tool worker crashes | Kafka retains messages at the group's committed offset | Automatic on restart |
| Orchestrator crashes | Plan state is in LevelDB, not memory | Resumes from `stepIndex` |
| Duplicate delivery | Workers filter by `toolName`; the orchestrator drops results for conversations it no longer tracks | Silent drop |
| Tool returns an error | `PlanFailed` + a `dead-letter-queue` entry; the UI shows the reason | Manual inspection |
| LLM unavailable | Ollama → OpenAI → regex planner | Automatic |

Reproduction steps: [`docs/resilience-tests/resilience-demo.md`](docs/resilience-tests/resilience-demo.md)

### Degradation observed end to end

A live run with `llama3.2:1b` and a placeholder OpenAI key exercised all three router tiers in one request:

```
[llm]    ollama-unavailable reason="The operation timed out." fallback=openai
[router] mode=regex-fallback reason="401 Incorrect API key provided: your_ope***here"
[router] plan=[math] input="what is 25 * 4"
```

Ollama timed out on the ~1,900-token router prompt, OpenAI rejected the placeholder key, and the regex planner produced the correct plan. The pipeline then completed normally and returned `100`. Measured for that request: `workerLatency=324ms`, `synthesizerLatency=6,245ms` — synthesis ran on the same local model without trouble, since its prompt is far shorter.

The RAG worker happened to be down during that run (its Python dependencies were still installing) and the pipeline was unaffected, which is the isolation property the topic-per-tool design is meant to give.

---

## Setup

### Requirements

- **macOS, Linux, or WSL2** — `bun run start` resolves the Python virtualenv at `venv/bin/python` and `bun run stop` uses `pkill`, both POSIX-only
- Docker + docker-compose
- Bun 1.0+
- Python 3.12+
- Ollama *(optional — without it the system falls back to OpenAI)*

### One-time setup

```bash
cp .env.example .env
docker compose -f infra/docker-compose.yml up -d
bash infra/topics-final.sh        # 4 topics × 3 partitions
bun install

# Python RAG worker
cd src/python
python3 -m venv venv && source venv/bin/activate
pip install -r rag/requirements.txt
python rag/index_kb.py            # index data/products/ into ChromaDB
cd ../..
```

**On `.env`:** to run entirely on the local model, leave `OPENAI_API_KEY` at its placeholder value rather than deleting the line — the OpenAI client is constructed when the module loads, even when it is never called. To use OpenAI, replace the placeholder with a real key.

### Run

```bash
bun run start                     # launches all 10 services in the background
                                  # logs → scripts/logs/final-project-services/
```

Open **http://localhost:3001**. For hot-reload development use `bun run web:dev` (Vite on port 5173, proxying `/ws` to 3001).

```bash
bun run stop
```

---

## Measured performance

27 live queries, 0 failures. Full report: [`docs/benchmark.md`](docs/benchmark.md)

| Stage | Average | Notes |
|---|---|---|
| Router (LLM plan) | ~1,113 ms | 597 ms – 3,297 ms |
| Tool workers + Kafka round-trips | ~39 ms | simple tools ~14 ms, RAG ~65 ms |
| Synthesizer (LLM answer) | ~2,189 ms | 784 ms – 4,694 ms |
| **End to end** | **~3,341 ms** | 1,589 ms – 6,165 ms |

**Kafka pipeline overhead stays under 50 ms regardless of plan complexity. LLM inference accounts for roughly 99% of end-to-end latency** — the useful conclusion being that no amount of Kafka tuning would move the number, while a faster model or streaming output would.

Consumer lag measured 0 across all ten groups after the run.

---

## Project structure

```
├── infra/
│   ├── docker-compose.yml          Kafka (KRaft), Ollama, ChromaDB
│   └── topics-final.sh             creates 4 topics × 3 partitions
├── scripts/
│   ├── start.ts                    waits for Kafka, pulls the model, starts 10 services
│   └── stop.ts
├── shared/
│   ├── kafka/client.ts             KafkaJS wrapper with retry + graceful shutdown
│   ├── llm/openai.ts               Ollama-first client with OpenAI fallback
│   ├── prompts/                    few-shot router prompt, synthesis prompt
│   ├── schemas/                    8 event contracts — TypeScript + JSON Schema
│   ├── state/planStore.ts          LevelDB plan persistence
│   ├── state/historyStore.ts       per-session conversation history
│   └── topics.ts
├── src/
│   ├── frontend/                   React 19 + Vite + Tailwind v4 chat UI
│   ├── node/
│   │   ├── core/                   routerService, webServer
│   │   ├── orchestration/          orchestrator, aggregator, answerSynthesizer
│   │   └── apps/                   math, weather, exchange, chat workers
│   └── python/rag/                 index_kb.py, rag_retriever.py
└── data/products/                  RAG knowledge base (5 plain-text documents)
```

---

## Scope and known limits

Stated plainly, because they are deliberate trade-offs rather than oversights:

- **`weatherApp` and `exchangeApp` serve static data.** No external API is called. They exist to exercise multi-step planning and placeholder chaining, not to be real integrations.
- **`generalChatApp` is rule-based**, not an LLM — pattern matching over a response table with a guardrail layer in front.
- **Steps execute sequentially.** The orchestrator dispatches one step at a time even when steps are independent, which costs wall-clock time on multi-tool plans.
- **State is node-local.** LevelDB and `history.json` both live on the machine running the process, so a second orchestrator instance would not see the first one's plans.
- **Idempotency is partial.** A duplicate result arriving after a plan completes is dropped, since the plan record is deleted on completion. A duplicate arriving mid-plan would be counted twice — there is no dedup key.
- **One session per WebSocket connection.** Refreshing the browser starts a new session and loses conversation history.
- **Plan validation is structural, not semantic.** The router checks that every step names a known tool and carries an args object, but not that a `{{step_N.result}}` placeholder refers to an earlier step that exists. Structurally valid nonsense passes, and the regex fallback never fires.
- **The synthesizer has no fallback.** The router degrades across three tiers (Ollama → OpenAI → regex); `answerSynthesizer` calls the LLM without a catch, so if every provider fails the pipeline produces no final answer.
- **Anti-hallucination is prompt-level only.** `synthesisPrompt` instructs the model not to add information absent from the tool results, but nothing enforces it. In a local run with `llama3.2:1b`, a product question was misrouted to the `chat` tool, and the synthesizer produced a fluent, entirely fabricated spec sheet from a non-answer — while the correct data sat in the vector store, un-queried. The regex planner would have routed it correctly, but it only runs when the LLM *fails*, not when it succeeds badly.

---

## Documentation

| Document | Contents |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Detailed architecture and per-event schemas |
| [`docs/benchmark.md`](docs/benchmark.md) | Latency, throughput, cost and consumer lag |
| [`docs/resilience-tests/resilience-demo.md`](docs/resilience-tests/resilience-demo.md) | Crash and recovery reproduction |
| [`docs/demo-scenarios/demo-scenarios.md`](docs/demo-scenarios/demo-scenarios.md) | Walkthrough with real log output |
| [`docs/execution-log.txt`](docs/execution-log.txt) | Full execution log, 14 scenarios |

---

## Stack

| Layer | Technology |
|---|---|
| Broker | Apache Kafka 3.8.0 (KRaft) |
| Runtime | Bun + TypeScript |
| LLM | Ollama (Llama 3.2) with OpenAI `gpt-4o-mini` fallback |
| Retrieval | ChromaDB + `sentence-transformers/all-MiniLM-L6-v2` |
| State | LevelDB |
| Frontend | React 19, Vite, Tailwind CSS v4 |
| Python worker | `kafka-python`, `chromadb`, `sentence-transformers` |
| Infrastructure | Docker Compose |
