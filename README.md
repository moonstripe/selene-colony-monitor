# Selene Colony Monitor

A standalone, **read-only** dashboard for the Project Selene rover agent. It lives
outside the evaluated `project-selene` repo and modifies none of it — it only
consumes the rover's already-published host ports.

> Companion to a take-home infrastructure-assessment exercise. The agent it observes
> maps a 12-pod lunar colony, builds a dependency graph, and reports systemic risk.

## What it does
- **Live phase monitor** via Server-Sent Events: `idle → mapping → mapped → reporting → report_ready` (and `error`/`offline`), with an elapsed timer while a job runs.
- **Three graph views** rendered from `map.json`, switchable from the panel header:
  - **Material** — declared dependencies, nodes sized/reddened by how depended-upon they are, edges colored by criticality.
  - **Cascade** — nodes sized by individual blast radius (how many pods their failure takes offline).
  - **Coordination** — the comms / authority relay collapsed to pod-level sender/receiver roots.
- **Brief / Report / Chat tabs** in the right panel:
  - **Brief** — deterministic topology snapshot (core cycle, key players, buffers, coordination).
  - **Report** — `report.md` rendered as formatted HTML, auto-refreshed when a new report lands.
  - **Chat** — ask the agent about the colony; it answers by calling the same tools the reporting agent uses, and the UI streams each tool call + result so you can watch the agentic loop.
- **⚑ Key Findings** — highlights the top blast-radius pods in the graph and scrolls the report to them.
- **Node click** — detail card (role, dependencies w/ criticality, supplies, specs) + dims everything except that pod and its neighbors.
- **💥 Failure injection** — a client-side what-if: click pods to "fail" them and watch the cascade (red=injected, orange=cascaded, green=surviving). Never touches the live colony.
- **Trigger buttons** for Run Mapping / Run Report (proxied to the rover).

## How it works
The rover's job API is poll-only (`202` running / `200` done / `500` error) and sets
no CORS headers. The dashboard polls it server-side and re-emits an SSE stream to
the browser, and proxies `/api/map`, `/api/report`, `/api/gateway` so the browser
never talks to the rover directly. Zero changes to the rover or `docker-compose.yml`.

Chat and failure-injection reuse the deliverable's `engine`/`tools` by importing them
**read-only** (no files changed); the chat runs the tool-use loop in this process and
streams events. The cascade math mirrors the engine, which remains source of truth.

## Prerequisites
- [`uv`](https://docs.astral.sh/uv/) — runs the script and resolves its inline
  (PEP-723) dependencies automatically. No manual `pip install` or venv needed.
- The **Project Selene colony stack running** (the rover at `:8080`, gateway at `:3000`).
  Without it the dashboard still loads but shows `ROVER OFFLINE`.
- **For the Chat tab only:** an LLM API key (Anthropic `sk-ant-…` or OpenAI `sk-…`)
  and access to the rover's agent code (for the tool definitions). Everything else —
  monitor, graphs, report, failure injection — works with no key.

### Recommended directory layout
The chat feature imports the agent code from a sibling checkout by default
(`../project-selene/rover`). Lay the two repos out side by side:

```
your-workspace/
├── project-selene/        # the evaluated repo (rover + docker-compose.yml)
└── selene-colony-monitor/ # this repo
```

If your layout differs, set `ROVER_CODE_PATH` (see Configuration).

## Quick start

```bash
# 1. Bring up the colony stack (from the project-selene repo)
cd ../project-selene
LLM_API_KEY=sk-ant-... docker compose up --build -d

# 2. Run the dashboard (from this repo)
cd ../selene-colony-monitor
uv run dashboard.py            # serves http://localhost:8090
```

Then open **http://localhost:8090**. Use the **Run Mapping** and **Run Report**
buttons (or trigger the rover directly) and watch the phase monitor advance.

To enable the **Chat** tab, make the same key visible to the dashboard process —
either it inherits it from the project `.env`, or pass it explicitly:

```bash
LLM_API_KEY=sk-ant-... uv run dashboard.py
```

## Configuration

All optional. Flags:

| Flag | Default | Purpose |
|---|---|---|
| `--port` | `8090` | Port to serve on |
| `--host` | `127.0.0.1` | Bind address |

Environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `ROVER_URL` | `http://localhost:8080` | Rover job API the dashboard polls/proxies |
| `GATEWAY_URL` | `http://localhost:3000` | Colony gateway (proxied for the intro view) |
| `ROVER_CODE_PATH` | `../project-selene/rover` | Locates the agent `engine`/`tools` for chat + failure injection |
| `LLM_API_KEY` | *(falls back to project `.env`)* | Key for the Chat tool-use loop |
| `LLM_MODEL` | `claude-sonnet-4-6` / `gpt-4o` | Override the model per provider |
| `DASHBOARD_CHAT_REQUEST_TIMEOUT_S` | `20` | Per-request timeout for chat |
| `DASHBOARD_CHAT_DEADLINE_S` | `45` | Overall deadline for one chat turn |

**Provider is auto-detected from the key:** `sk-ant-…` → Anthropic, anything else → OpenAI.

**Key resolution order:** the dashboard uses `LLM_API_KEY` from its own environment
if set; otherwise it reads the project `.env` (at `ROVER_CODE_PATH/../.env`),
preferring `LLM_API_KEY`, then `ANTHROPIC_API_KEY`, then `OPENAI_API_KEY`.

## Troubleshooting
- **`ROVER OFFLINE` in the header** — the colony stack isn't up, or `ROVER_URL` is
  wrong. Confirm `curl localhost:8080/health` returns `{"status":"ok"}`.
- **Chat tab errors with "agent code not importable"** — `ROVER_CODE_PATH` doesn't
  point at the rover directory containing `agent/`. Set it explicitly.
- **Chat says no key** — supply `LLM_API_KEY` (env or project `.env`); the rest of
  the dashboard runs fine without one.
- **Edited the inline HTML/JS and don't see the change** — restart the process
  (it's served from one file; there's no `--reload`).
- **Port already in use** — pass `--port`, or free `8090`
  (`lsof -ti:8090 | xargs kill -9`).

## Security
No API keys live in this repo. The key is supplied at runtime via `LLM_API_KEY` (or a
local, git-ignored `.env`). Don't commit keys — `.env` and `*.key` are git-ignored.
