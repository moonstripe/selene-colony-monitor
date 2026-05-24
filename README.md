# Selene Colony Monitor

A standalone, **read-only** dashboard for the Project Selene rover agent. It lives
outside the evaluated `project-selene` repo and modifies none of it — it only
consumes the rover's already-published host ports.

> Companion to a take-home infrastructure-assessment exercise. The agent it observes
> maps a 12-pod lunar colony, builds a dependency graph, and reports systemic risk.

## What it does
- **Live phase monitor** via Server-Sent Events: `idle → mapping → mapped → reporting → report_ready` (and `error`/`offline`), with an elapsed timer while a job runs.
- **Dependency graph** rendered from `map.json` — nodes sized/reddened by how depended-upon they are (⚠ marks the top hubs), edges colored by criticality.
- **Latest report** — `report.md` rendered as formatted HTML, auto-refreshed when a new report lands.
- **⚑ Key Findings** — highlights the top blast-radius pods in the graph and scrolls the report to them.
- **Node click** — detail card (role, dependencies w/ criticality, supplies, specs) + dims everything except that pod and its neighbors.
- **💥 Failure injection** — a client-side what-if: click pods to "fail" them and watch the cascade (red=injected, orange=cascaded, green=surviving). Never touches the live colony.
- **💬 Chat with the agent** — ask questions about the colony; the agent answers by calling the same tools the reporting agent uses, and the UI streams each tool call + result so you can watch the agentic loop.
- **Trigger buttons** for Run Mapping / Run Report (proxied to the rover).

## How it works
The rover's job API is poll-only (`202` running / `200` done / `500` error) and sets
no CORS headers. The dashboard polls it server-side and re-emits an SSE stream to
the browser, and proxies `/api/map`, `/api/report`, `/api/gateway` so the browser
never talks to the rover directly. Zero changes to the rover or `docker-compose.yml`.

Chat and failure-injection reuse the deliverable's `engine`/`tools` by importing them
**read-only** (no files changed); the chat runs the tool-use loop in this process and
streams events. The cascade math mirrors the engine, which remains source of truth.

## Run
First bring up the colony stack in the project repo (`docker compose up --build -d`), then:

```bash
uv run dashboard.py            # serves http://localhost:8090
```

Options: `--port`, `--host`; env `ROVER_URL` (default `http://localhost:8080`),
`GATEWAY_URL` (default `http://localhost:3000`). For chat: `ROVER_CODE_PATH`
(default `../project-selene/rover`) locates the agent code, and the LLM key is read
from `LLM_API_KEY` or falls back to the project `.env` (prefers `LLM_API_KEY`, then
`ANTHROPIC_API_KEY`, then `OPENAI_API_KEY`).

## Security
No API keys live in this repo. The key is supplied at runtime via `LLM_API_KEY` (or a
local, git-ignored `.env`). Don't commit keys — `.env` and `*.key` are git-ignored.
