# /// script
# requires-python = ">=3.10"
# dependencies = ["fastapi", "uvicorn", "httpx", "networkx", "anthropic", "openai"]
# ///
"""Selene Colony Monitor — a standalone, read-only dashboard.

This lives OUTSIDE the evaluated project-selene repo and touches none of it. It
only consumes the rover's already-published host ports:
  - rover    http://localhost:8080   (/health, POST /map, GET /get-map, POST /report, GET /get-report)
  - gateway  http://localhost:3000

It polls those endpoints server-side and re-emits a live Server-Sent Events
stream to the browser, proxies the artifacts, and computes a deterministic
analysis bundle by importing the rover engine read-only.
"""

import argparse
import asyncio
import json
import os
import pathlib
import sys
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

ROVER = os.environ.get("ROVER_URL", "http://localhost:8080")
GATEWAY = os.environ.get("GATEWAY_URL", "http://localhost:3000")

ROVER_CODE = os.environ.get("ROVER_CODE_PATH") or str(
    pathlib.Path(__file__).resolve().parent.parent / "project-selene" / "rover"
)
AGENT_OK, AGENT_ERR = False, None
try:
    if ROVER_CODE not in sys.path:
        sys.path.insert(0, ROVER_CODE)
    from agent.engine import ColonyEngine
    from agent import tools as agent_tools

    AGENT_OK = True
except Exception as e:
    ColonyEngine, agent_tools, AGENT_ERR = None, None, f"{type(e).__name__}: {e}"

app = FastAPI()
_MAP_CACHE = {"doc": None}


async def _probe(client: httpx.AsyncClient) -> dict:
    state = {
        "map_http": None,
        "report_http": None,
        "map_started": None,
        "report_started": None,
        "online": True,
    }
    try:
        for job, path in (("map", "/get-map"), ("report", "/get-report")):
            r = await client.get(ROVER + path)
            state[f"{job}_http"] = r.status_code
            if r.status_code == 202:
                try:
                    state[f"{job}_started"] = r.json().get("started_at")
                except Exception:
                    pass
    except (httpx.TransportError, httpx.TimeoutException):
        state["online"] = False

    mh, rh = state["map_http"], state["report_http"]
    if not state["online"]:
        phase = "offline"
    elif mh == 202:
        phase = "mapping"
    elif rh == 202:
        phase = "reporting"
    elif mh == 500 or rh == 500:
        phase = "error"
    elif rh == 200:
        phase = "report_ready"
    elif mh == 200:
        phase = "mapped"
    else:
        phase = "idle"
    state["phase"] = phase
    return state


@app.get("/events")
async def events():
    async def gen():
        last_sig = None
        async with httpx.AsyncClient(timeout=4.0) as client:
            while True:
                st = await _probe(client)
                sig = (
                    st["phase"],
                    st["map_http"],
                    st["report_http"],
                    st["map_started"],
                    st["report_started"],
                )
                if sig != last_sig:
                    yield f"data: {json.dumps(st)}\n\n"
                    last_sig = sig
                else:
                    yield ": heartbeat\n\n"
                await asyncio.sleep(1.5)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _proxy(method: str, url: str) -> Response:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.request(method, url)
        return Response(
            content=r.content,
            status_code=r.status_code,
            media_type=r.headers.get("content-type", "application/json"),
        )
    except (httpx.TransportError, httpx.TimeoutException) as e:
        return Response(
            content=json.dumps({"error": f"rover unreachable: {e}"}),
            status_code=503,
            media_type="application/json",
        )


@app.get("/api/map")
async def api_get_map():
    return await _proxy("GET", ROVER + "/get-map")


@app.get("/api/report")
async def api_get_report():
    return await _proxy("GET", ROVER + "/get-report")


@app.post("/api/map")
async def api_post_map():
    return await _proxy("POST", ROVER + "/map")


@app.post("/api/report")
async def api_post_report():
    return await _proxy("POST", ROVER + "/report")


@app.get("/api/gateway")
async def api_gateway():
    return await _proxy("GET", GATEWAY + "/")


async def _load_engine():
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(ROVER + "/get-map")
        if r.status_code == 200:
            _MAP_CACHE["doc"] = r.json()
    except (httpx.TransportError, httpx.TimeoutException):
        pass
    return ColonyEngine(_MAP_CACHE["doc"]) if _MAP_CACHE["doc"] else None


@app.get("/api/analysis")
async def api_analysis():
    if not AGENT_OK:
        return JSONResponse(
            {"error": f"agent code not importable from {ROVER_CODE}: {AGENT_ERR}"},
            status_code=503,
        )
    engine = await _load_engine()
    if engine is None:
        return JSONResponse(
            {"error": "no map available yet — run mapping first"}, status_code=404
        )
    return JSONResponse(engine.facts())


def _llm_key():
    if os.environ.get("LLM_API_KEY"):
        return os.environ["LLM_API_KEY"]
    envp = pathlib.Path(ROVER_CODE).parent / ".env"
    vals = {}
    if envp.exists():
        for line in envp.read_text().splitlines():
            line = line.strip()
            for name in ("LLM_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY"):
                if line.startswith(name + "="):
                    vals[name] = line.split("=", 1)[1].strip()
    for name in ("LLM_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY"):
        if vals.get(name):
            return vals[name]
    return None


CHAT_REQUEST_TIMEOUT_S = float(os.environ.get("DASHBOARD_CHAT_REQUEST_TIMEOUT_S", "20"))
CHAT_DEADLINE_S = float(os.environ.get("DASHBOARD_CHAT_DEADLINE_S", "45"))


def _remaining_chat_timeout(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("dashboard chat exceeded overall deadline")
    return min(CHAT_REQUEST_TIMEOUT_S, remaining)


CHAT_SYSTEM = (
    "You are the Selene colony infrastructure analyst. Answer questions about the colony's material "
    "dependencies, hard-failure topology, buffers, comms, and coordination hierarchy by calling the "
    "available tools to gather evidence, then giving a concise, specific answer grounded in that data — "
    "cite pod ids, dates, and numbers. Prefer tools over assumptions. Keep answers short."
)


def _chat_anthropic(key, engine, history, message):
    from anthropic import Anthropic

    client = Anthropic(api_key=key, timeout=CHAT_REQUEST_TIMEOUT_S, max_retries=0)
    model = os.environ.get("LLM_MODEL", "claude-sonnet-4-6")
    spec = [
        {
            "name": t["name"],
            "description": t["description"],
            "input_schema": t["input_schema"],
        }
        for t in agent_tools.TOOL_SPECS
    ]
    messages = [{"role": m["role"], "content": m["content"]} for m in history]
    messages.append({"role": "user", "content": message})
    deadline = time.monotonic() + CHAT_DEADLINE_S
    for _ in range(12):
        timeout = _remaining_chat_timeout(deadline)
        resp = client.messages.create(
            model=model,
            max_tokens=2000,
            system=CHAT_SYSTEM,
            tools=spec,
            messages=messages,
            timeout=timeout,
        )
        messages.append({"role": "assistant", "content": resp.content})
        if resp.stop_reason != "tool_use":
            yield {
                "type": "answer",
                "content": "".join(b.text for b in resp.content if b.type == "text"),
            }
            return
        results = []
        for b in resp.content:
            if b.type == "tool_use":
                yield {"type": "tool_call", "name": b.name, "input": b.input}
                out = json.dumps(
                    agent_tools.dispatch(engine, b.name, b.input), default=str
                )
                yield {"type": "tool_result", "name": b.name, "preview": out[:400]}
                results.append(
                    {"type": "tool_result", "tool_use_id": b.id, "content": out}
                )
        messages.append({"role": "user", "content": results})
    yield {"type": "error", "error": "max turns reached"}


def _chat_openai(key, engine, history, message):
    from openai import OpenAI

    client = OpenAI(api_key=key, timeout=CHAT_REQUEST_TIMEOUT_S, max_retries=0)
    model = os.environ.get("LLM_MODEL", "gpt-4o")
    spec = [
        {
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t["description"],
                "parameters": t["input_schema"],
            },
        }
        for t in agent_tools.TOOL_SPECS
    ]
    messages = [{"role": "system", "content": CHAT_SYSTEM}]
    messages += [{"role": m["role"], "content": m["content"]} for m in history]
    messages.append({"role": "user", "content": message})
    deadline = time.monotonic() + CHAT_DEADLINE_S
    for _ in range(12):
        timeout = _remaining_chat_timeout(deadline)
        resp = client.chat.completions.create(
            model=model, messages=messages, tools=spec, timeout=timeout
        )
        m = resp.choices[0].message
        if not m.tool_calls:
            yield {"type": "answer", "content": m.content or ""}
            return
        messages.append(
            {"role": "assistant", "content": m.content, "tool_calls": m.tool_calls}
        )
        for tc in m.tool_calls:
            args = json.loads(tc.function.arguments or "{}")
            yield {"type": "tool_call", "name": tc.function.name, "input": args}
            out = json.dumps(
                agent_tools.dispatch(engine, tc.function.name, args), default=str
            )
            yield {
                "type": "tool_result",
                "name": tc.function.name,
                "preview": out[:400],
            }
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": out})
    yield {"type": "error", "error": "max turns reached"}


def _chat_stream(engine, history, message):
    key = _llm_key()
    if not key:
        yield {
            "type": "error",
            "error": "LLM_API_KEY not set (and none found in project .env)",
        }
        return
    backend = _chat_anthropic if key.startswith("sk-ant-") else _chat_openai
    try:
        yield from backend(key, engine, history, message)
    except Exception as e:
        yield {"type": "error", "error": f"{type(e).__name__}: {e}"}


@app.post("/api/chat")
async def api_chat(req: Request):
    body = await req.json()

    def errstream(msg):
        async def g():
            yield json.dumps({"type": "error", "error": msg}) + "\n"

        return StreamingResponse(g(), media_type="application/x-ndjson")

    if not AGENT_OK:
        return errstream(f"agent code not importable from {ROVER_CODE}: {AGENT_ERR}")
    engine = await _load_engine()
    if engine is None:
        return errstream("no map available yet — run mapping first")

    def gen():
        for ev in _chat_stream(
            engine, body.get("history", []), body.get("message", "")
        ):
            yield json.dumps(ev, default=str) + "\n"
        yield json.dumps({"type": "done"}) + "\n"

    return StreamingResponse(gen(), media_type="application/x-ndjson")


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Selene Colony Monitor</title>
<script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<style>
  :root{
    --paper:#f7f3ea;
    --panel:#fffdf8;
    --line:#d8d0c2;
    --ink:#1f1b18;
    --muted:#6e655c;
    --soft:#8a8075;
    --core:#a9471b;
    --relay:#2c5d8a;
    --island:#38745a;
    --risk:#7d2222;
    --warn:#9a6a10;
    --bg:#efe8da;
  }
  *{box-sizing:border-box;}
  html,body{height:100%;}
  body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 "Iowan Old Style","Palatino Linotype","Book Antiqua",Georgia,serif;}
  button,input{font:inherit;}
  header{display:grid;grid-template-columns:auto 1fr auto;gap:16px;align-items:end;padding:18px 22px 14px;border-bottom:1px solid var(--line);background:var(--panel);}
  h1{margin:0;font-size:24px;font-weight:600;letter-spacing:.02em;}
  .deck{color:var(--muted);font-size:13px;max-width:64ch;}
  .statusline{display:flex;align-items:center;gap:8px;color:var(--muted);font-size:12px;white-space:nowrap;}
  .dot{width:8px;height:8px;border-radius:50%;background:#9f968c;display:inline-block;}
  .dot.live{background:var(--island);}
  .topbar{display:grid;grid-template-columns:1fr auto;gap:18px;padding:14px 22px;border-bottom:1px solid var(--line);background:var(--panel);}
  .phaseblock{display:flex;align-items:baseline;gap:14px;min-width:0;}
  .phase{font-size:26px;font-weight:600;letter-spacing:.01em;}
  .phase.idle,.phase.mapped{color:var(--relay);} .phase.mapping,.phase.reporting{color:var(--warn);} .phase.report_ready{color:var(--island);} .phase.error,.phase.offline{color:var(--risk);}
  .elapsed,.gateway{color:var(--muted);font-size:13px;}
  .controls{display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end;}
  button{background:transparent;border:1px solid var(--line);color:var(--ink);padding:7px 12px;border-radius:4px;cursor:pointer;}
  button:hover:not(:disabled){border-color:var(--ink);}
  button:disabled{opacity:.45;cursor:not-allowed;}
  button.active{background:#f1e2d8;border-color:var(--core);color:var(--core);}
  .mode.active,.tab.active{border-color:var(--ink);background:#f5efe3;}
  .metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;padding:16px 22px;border-bottom:1px solid var(--line);background:var(--panel);}
  .metric{padding-bottom:8px;border-bottom:1px solid var(--line);}
  .metric .label{font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--soft);}
  .metric .value{font-size:28px;line-height:1.05;margin-top:4px;font-variant-numeric:tabular-nums;}
  .metric .note{font-size:12px;color:var(--muted);margin-top:4px;}
  main{display:grid;grid-template-columns:minmax(420px,1.2fr) minmax(360px,.9fr);gap:0;height:calc(100vh - 214px);}
  .panel{min-width:0;min-height:0;background:var(--panel);}
  .panel + .panel{border-left:1px solid var(--line);}
  .panelhead{display:flex;align-items:center;justify-content:space-between;gap:14px;padding:10px 16px;border-bottom:1px solid var(--line);}
  .panelhead h2,.tabs button{margin:0;font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);font-weight:600;}
  .graphmodes,.tabs{display:flex;gap:6px;flex-wrap:wrap;}
  .mode,.tab{padding:5px 9px;font-size:11px;letter-spacing:.08em;text-transform:uppercase;}
  #graph-wrap{position:relative;height:100%;min-height:0;}
  #graph{height:100%;}
  .overlay{position:absolute;left:12px;top:12px;max-width:min(430px,70%);background:rgba(255,253,248,.95);border:1px solid var(--line);padding:10px 12px;font-size:12px;line-height:1.45;display:none;}
  .overlay b{color:var(--ink);} .overlay .core{color:var(--core);} .overlay .relay{color:var(--relay);} .overlay .island{color:var(--island);}
  .legend{position:absolute;left:12px;bottom:12px;background:rgba(255,253,248,.95);border:1px solid var(--line);padding:8px 10px;font-size:11px;line-height:1.45;color:var(--muted);max-width:min(360px,68%);}
  .legend b{color:var(--ink);}
  .nodecard{position:absolute;right:12px;top:12px;width:300px;max-height:calc(100% - 24px);overflow:auto;background:rgba(255,253,248,.98);border:1px solid var(--line);padding:12px 14px 14px;font-size:12px;box-shadow:0 8px 24px rgba(42,32,24,.08);display:none;}
  .nodecard .close{float:right;border:none;padding:0 4px;background:none;font-size:18px;color:var(--muted);}
  .nodecard h3{margin:0 0 2px;font-size:18px;} .nodecard h3 span{font-size:11px;color:var(--muted);font-weight:400;}
  .nodecard .role{color:var(--relay);margin-bottom:4px;} .nodecard .meta{color:var(--muted);font-size:11px;margin-bottom:10px;}
  .nodecard h4{margin:11px 0 4px;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:var(--soft);}
  .nodecard ul{list-style:none;padding:0;margin:0;} .nodecard li{padding:2px 0;} .none{color:var(--muted);}
  .crit{display:inline-block;margin-left:6px;padding:0 4px;font-size:9px;letter-spacing:.06em;text-transform:uppercase;border:1px solid var(--line);border-radius:2px;}
  .crit.high{border-color:#d09090;color:var(--risk);} .crit.medium{border-color:#d6b777;color:var(--warn);} .crit.low{color:var(--soft);}
  .tabpanes{height:calc(100% - 45px);} .tabpane{height:100%;display:none;overflow:auto;} .tabpane.active{display:block;}
  #brief{padding:14px 16px 24px;} #report{padding:10px 22px 60px;} #chatpane{display:none;flex-direction:column;height:100%;}
  #brief .lede{margin:0 0 14px;padding-bottom:10px;border-bottom:1px solid var(--line);color:var(--ink);} #brief .lede span{color:var(--muted);}
  .briefgrid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;margin-bottom:16px;}
  .mini h3,.section h3{margin:0 0 8px;font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:var(--soft);} 
  .mini{padding-bottom:10px;border-bottom:1px solid var(--line);} .mini .big{font-size:24px;line-height:1.05;font-variant-numeric:tabular-nums;} .mini p{margin:6px 0 0;font-size:12px;color:var(--muted);} 
  .section{margin-top:14px;padding-top:12px;border-top:1px solid var(--line);} 
  table{width:100%;border-collapse:collapse;} th,td{padding:6px 4px;vertical-align:top;text-align:left;} th{font-size:11px;letter-spacing:.06em;text-transform:uppercase;color:var(--soft);font-weight:600;border-bottom:1px solid var(--line);} td{border-bottom:1px solid #ebe4d8;font-size:12.5px;}
  .mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;}
  .chip{display:inline-block;padding:1px 6px;border:1px solid var(--line);border-radius:999px;font-size:10px;letter-spacing:.06em;text-transform:uppercase;color:var(--muted);margin-right:6px;}
  .corechip{border-color:#d7a18b;color:var(--core);} .relaychip{border-color:#9db8cf;color:var(--relay);} .islandchip{border-color:#9dbfae;color:var(--island);} .bufferchip{border-color:#d9c697;color:var(--warn);}
  #report h1{font-size:24px;border-bottom:1px solid var(--line);padding-bottom:10px;} #report h2{font-size:18px;margin-top:26px;color:var(--ink);} #report h3{font-size:14px;color:var(--ink);} #report code{background:#f3ede1;padding:1px 4px;border-radius:3px;} #report blockquote{margin:10px 0;padding:0 0 0 12px;border-left:2px solid var(--line);color:var(--muted);} #report a{color:var(--relay);} #report th,#report td{padding:6px 8px;} 
  #chatpane.active{display:flex;} #chatlog{flex:1;overflow:auto;padding:14px 16px;display:flex;flex-direction:column;gap:8px;} .chathint{font-size:12px;color:var(--muted);} .msg{max-width:90%;padding:8px 10px;border:1px solid var(--line);font-size:13px;line-height:1.45;} .msg.user{align-self:flex-end;background:#f2e8dc;} .msg.bot{align-self:flex-start;background:#fffdf8;} .toolchip{align-self:flex-start;border:1px solid var(--line);padding:5px 8px;font-size:11px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;background:#f8f3ea;max-width:94%;} .toolchip .res{display:block;margin-top:4px;color:var(--muted);white-space:pre-wrap;word-break:break-word;} .chatbar{display:flex;gap:8px;padding:10px 12px;border-top:1px solid var(--line);} .chatbar input{flex:1;padding:8px 10px;border:1px solid var(--line);background:transparent;color:var(--ink);} .chatbar input:focus{outline:none;border-color:var(--ink);} 
  .empty{display:flex;align-items:center;justify-content:center;color:var(--muted);} 
  @media (max-width:1180px){ .metrics{grid-template-columns:repeat(2,minmax(0,1fr));} main{grid-template-columns:1fr;height:auto;min-height:calc(100vh - 214px);} .panel + .panel{border-left:none;border-top:1px solid var(--line);} }
  @media (max-width:760px){ header{grid-template-columns:1fr;align-items:start;} .topbar{grid-template-columns:1fr;} .controls{justify-content:flex-start;} .metrics,.briefgrid{grid-template-columns:1fr;} .overlay,.legend,.nodecard{max-width:calc(100% - 24px);width:auto;right:12px;} }
</style>
</head>
<body>
<header>
  <div>
    <h1>Selene Colony Monitor</h1>
    <div class="deck">Read-only analytical companion to the rover. Material core, cascade backbone, relay hierarchy, and buffer clocks are derived from the rover’s deterministic engine.</div>
  </div>
  <div></div>
  <div class="statusline"><span class="dot" id="cdot"></span><span id="cstat">stream offline</span></div>
</header>
<div class="topbar">
  <div>
    <div class="phaseblock">
      <div class="phase idle" id="phase">IDLE</div>
      <div class="elapsed" id="elapsed"></div>
    </div>
    <div class="gateway" id="colony">connecting…</div>
  </div>
  <div class="controls">
    <button id="btnMap">Run Mapping</button>
    <button id="btnReport" disabled>Run Report</button>
    <button id="btnStory" disabled>Core & Relay</button>
    <button id="btnInject" disabled>Failure Injection</button>
  </div>
</div>
<section class="metrics">
  <div class="metric">
    <div class="label">Core Blast Radius</div>
    <div class="value" id="metricBlast">—</div>
    <div class="note" id="metricBlastNote">run mapping</div>
  </div>
  <div class="metric">
    <div class="label">Artemis Relay Load</div>
    <div class="value" id="metricRelay">—</div>
    <div class="note" id="metricRelayNote">run mapping</div>
  </div>
  <div class="metric">
    <div class="label">Fastest Buffer Window</div>
    <div class="value" id="metricBuffer">—</div>
    <div class="note" id="metricBufferNote">run mapping</div>
  </div>
  <div class="metric">
    <div class="label">Baseline Fragmentation</div>
    <div class="value" id="metricFrag">—</div>
    <div class="note" id="metricFragNote">directed dependency graph</div>
  </div>
</section>
<main>
  <div class="panel">
    <div class="panelhead">
      <h2 id="graphTitle">Topology Graph</h2>
      <div class="graphmodes">
        <button class="mode active" id="modeMaterial">Material</button>
        <button class="mode" id="modeCascade">Cascade</button>
        <button class="mode" id="modeCoord">Coordination</button>
      </div>
    </div>
    <div id="graph-wrap" class="empty"><span id="gph">no map yet — run mapping</span></div>
  </div>
  <div class="panel">
    <div class="panelhead">
      <div class="modes">
        <button class="mode active" data-tab="brief">Brief</button>
        <button class="mode" data-tab="report">Report</button>
        <button class="mode" data-tab="chat">Chat</button>
      </div>
    </div>
    <div class="tabpanes">
      <div id="brief" class="tabpane active empty">no map yet — run mapping</div>
      <div id="report" class="tabpane empty">no report yet — run report</div>
      <div id="chatpane" class="tabpane">
        <div id="chatlog"><div class="chathint">Ask the agent about the colony. It answers by calling the same deterministic tools the reporting agent uses. Good prompts: <i>"Why is Artemis a relay bottleneck?"</i>, <i>"Which pods survive a core collapse?"</i>, <i>"What did the comms reveal that the dependency graph missed?"</i></div></div>
        <div class="chatbar"><input id="chatinput" placeholder="Ask the agent…" autocomplete="off"/><button id="chatsend">Send</button></div>
      </div>
    </div>
  </div>
</main>
<script>
const PHASE_LABEL={idle:"IDLE",mapping:"MAPPING",mapped:"MAP READY",reporting:"REPORTING",report_ready:"REPORT READY",error:"ERROR",offline:"ROVER OFFLINE"};
const MODE_META={
  material:{title:'Material Topology',legend:'<b>Material:</b> all declared dependencies. Node size = number of dependents. Core = rust, relay = blue, survivor islands = green. Edge width and tone track declared criticality.'},
  cascade:{title:'Hard Cascade Backbone',legend:'<b>Cascade:</b> only high-criticality material edges that propagate hard failure. Node size = single-pod blast radius. This is the colony\'s real kill-chain, not its full paperwork graph.'},
  coordination:{title:'Coordination Relay',legend:'<b>Coordination:</b> comms collapsed to pod-level sender/receiver roots. Node size = observed coordination traffic. Broadcast counts stay in the brief to avoid hiding the actual directed links.'}
};
let mapData=null, analysis=null, reportText='', mapLoaded=false, reportLoaded=false, timer=null;
let network=null, nodesDS=null, edgesDS=null, baseGraph=null, nodeCard=null, overlay=null, legend=null;
let graphMode='material', injectMode=false, injected=new Set(), currentTab='brief', chatHistory=[];

const $=id=>document.getElementById(id);
const esc=s=>(s||'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));

function setConn(live){ $('cdot').classList.toggle('live',live); $('cstat').textContent=live?'live':'stream offline'; }
function tickElapsed(startISO){ clearInterval(timer); const el=$('elapsed'); if(!startISO){el.textContent=''; return;} const start=new Date(startISO).getTime(); const upd=()=>{ const s=Math.max(0,Math.floor((Date.now()-start)/1000)); el.textContent=`${String(Math.floor(s/60)).padStart(2,'0')}:${String(s%60).padStart(2,'0')} elapsed`; }; upd(); timer=setInterval(upd,1000); }
function fmtNum(n){ return n==null?'—':new Intl.NumberFormat().format(n); }
function fmtList(arr){ return (arr&&arr.length)?arr.join(', '):'—'; }
function fmtDays(n){ return n==null?'—':`${n}d`; }
function fmtHours(n){ return n==null?'—':`${n}h`; }
function classifyNode(id){ const core=new Set((analysis&&analysis.topology_summary&&analysis.topology_summary.core_cycle)||[]); const islands=new Set(islandPods()); if(core.has(id)) return 'core'; if(id==='artemis') return 'relay'; if(islands.has(id)) return 'island'; return 'support'; }
function nodeColors(kind){ if(kind==='core') return {background:'#c96f47',border:'#8f3d1c'}; if(kind==='relay') return {background:'#8eb4d8',border:'#2c5d8a'}; if(kind==='island') return {background:'#9dc4ae',border:'#38745a'}; return {background:'#d8d0c2',border:'#90867b'}; }
function keyPlayers(){ const rows=(analysis&&analysis.failure_impact_ranking)||[]; if(!rows.length) return []; const top=rows[0].total_offline; return rows.filter(r=>r.total_offline===top).map(r=>r.pod); }
function islandPods(){ const comps=(analysis&&analysis.topology_summary&&analysis.topology_summary.hard_cascade_components)||[]; if(!comps.length) return []; return comps.slice(1).filter(c=>c.length===1).map(c=>c[0]); }
function blastRadius(){ const rows=(analysis&&analysis.failure_impact_ranking)||[]; return rows.length?rows[0]:null; }
function fastBuffers(){ const rows=(analysis&&analysis.buffer_summary)||[]; const out=[]; rows.forEach(r=>{ const f=r.buffer_facts||{}; if(f.backup_power_hours!=null) out.push({pod:r.pod,label:`${r.pod} ${f.backup_power_hours}h`,hours:f.backup_power_hours}); if(f.oxygen_reserve_hours!=null) out.push({pod:r.pod,label:`${r.pod} ${f.oxygen_reserve_hours}h`,hours:f.oxygen_reserve_hours}); if(f.water_cover_days_estimate!=null) out.push({pod:r.pod,label:`${r.pod} ${f.water_cover_days_estimate}d`,hours:f.water_cover_days_estimate*24}); if(f.independent_power_days!=null) out.push({pod:r.pod,label:`${r.pod} ${f.independent_power_days}d`,hours:f.independent_power_days*24}); }); return out.sort((a,b)=>a.hours-b.hours); }
function actorRoot(name){ if(!name) return null; if(name==='all_pods') return null; const ids=Object.keys((mapData&&mapData.pods)||{}); if(ids.includes(name)) return name; for(const id of ids){ if(name===id || name.startsWith(id+'_')) return id; } return null; }

function applyState(st){
  setConn(true);
  const ph=$('phase'); ph.textContent=PHASE_LABEL[st.phase]||st.phase; ph.className='phase '+st.phase;
  const running=st.phase==='mapping'||st.phase==='reporting';
  tickElapsed(st.phase==='mapping'?st.map_started:(st.phase==='reporting'?st.report_started:null));
  $('btnMap').disabled=running; $('btnReport').disabled=running||!(st.map_http===200); $('btnStory').disabled=!(st.map_http===200); $('btnInject').disabled=!(st.map_http===200);
  if(st.phase==='mapping'){ mapLoaded=false; reportLoaded=false; }
  if(st.phase==='reporting'){ reportLoaded=false; }
  if(st.map_http===200 && !mapLoaded){ mapLoaded=true; loadMapAndAnalysis(); }
  if(st.report_http===200 && !reportLoaded){ reportLoaded=true; loadReport(); }
}

async function loadMapAndAnalysis(){
  const [mr,ar]=await Promise.all([fetch('/api/map'), fetch('/api/analysis')]);
  if(mr.status!==200 || ar.status!==200) return;
  mapData=await mr.json(); analysis=await ar.json();
  renderMetrics(); renderBrief(); renderGraph();
}

async function loadReport(){
  const r=await fetch('/api/report'); if(r.status!==200) return;
  reportText=await r.text(); const el=$('report'); el.classList.remove('empty'); el.innerHTML=marked.parse(reportText); if(currentTab==='report') el.scrollTop=0;
}

function renderMetrics(){
  const top=blastRadius();
  $('metricBlast').textContent=top?`${top.total_offline}/12`: '—';
  $('metricBlastNote').textContent=top?`${fmtNum(top.population_offline)} residents offline from any one of ${keyPlayers().join(', ')}`:'run mapping';
  const coord=analysis.coordination_summary||{};
  $('metricRelay').textContent=`${(coord.artemis_admin_inbound||[]).length}`;
  $('metricRelayNote').textContent=`Artemis inbound messages · ${(coord.artemis_broadcasts||[]).length} broadcasts`;
  const fb=fastBuffers();
  $('metricBuffer').textContent=fb.length?fb[0].label.split(' ').slice(1).join(' '):'—';
  $('metricBufferNote').textContent=fb.length?`fastest explicit buffer: ${fb[0].pod}`:'no explicit buffer facts';
  $('metricFrag').textContent=analysis.baseline_fragmentation!=null?analysis.baseline_fragmentation.toFixed(4):'—';
  $('metricFragNote').textContent='directed dependency graph';
}

function renderBrief(){
  const el=$('brief'); el.classList.remove('empty');
  const top=blastRadius();
  const coord=analysis.coordination_summary||{};
  const topo=analysis.topology_summary||{};
  const buffers=analysis.buffer_summary||[];
  const events=(analysis.notable_events||[]).slice(-5).reverse();
  const topRows=(analysis.failure_impact_ranking||[]).slice(0,6);
  const relayRows=(coord.receivers||[]).slice(0,5);
  const bufferRows=buffers.map(r=>({pod:r.pod,facts:Object.entries(r.buffer_facts||{}).map(([k,v])=>`${k}=${Array.isArray(v)?v.join('/') : v}`).join('; ')}));
  const islands=islandPods();
  el.innerHTML=`
    <p class="lede"><span>BLUF:</span> Selene is operating as a doubly centralized colony. The <span class="mono">${fmtList(topo.core_cycle)}</span> material core is collapse-equivalent, Artemis is the main coordination relay, and the remaining resilience is mostly time stored in local buffers.</p>
    <div class="briefgrid">
      <div class="mini">
        <h3>Destructive Core</h3>
        <div class="big">${top?`${top.total_offline} pods`:'—'}</div>
        <p>${top?`${fmtNum(top.population_offline)} residents offline if any one of ${keyPlayers().join(', ')} fails.`:'No failure ranking yet.'}</p>
      </div>
      <div class="mini">
        <h3>Coordination Relay</h3>
        <div class="big">${(coord.artemis_admin_inbound||[]).length}</div>
        <p>Inbound administrative messages to Artemis. Broadcasts observed: ${(coord.artemis_broadcasts||[]).length}. Flagged coordination-risk messages: ${(coord.flagged_messages||[]).length}.</p>
      </div>
      <div class="mini">
        <h3>Survivor Islands</h3>
        <div class="big">${islands.length}</div>
        <p>${islands.length?`${islands.join(', ')} remain outside the main hard-cascade component.`:'No detached survivor islands identified.'}</p>
      </div>
      <div class="mini">
        <h3>Shortest Clocks</h3>
        <div class="big">${fastBuffers().slice(0,2).map(x=>x.label).join(' · ')||'—'}</div>
        <p>These are not backups. They are the shortest explicit countdown timers in the map.</p>
      </div>
    </div>
    <div class="section">
      <h3>Topology</h3>
      <div><span class="chip corechip">core</span>${fmtList(topo.core_cycle)}<br><span class="chip relaychip">relay</span>artemis<br><span class="chip islandchip">islands</span>${fmtList(islands)}</div>
    </div>
    <div class="section">
      <h3>Single-Pod Blast Radius</h3>
      <table>
        <thead><tr><th>pod</th><th>offline</th><th>population</th><th>cascades to</th></tr></thead>
        <tbody>${topRows.map(r=>`<tr><td class="mono">${r.pod}</td><td>${r.total_offline}</td><td>${fmtNum(r.population_offline)}</td><td>${fmtList(r.cascaded_offline)}</td></tr>`).join('')}</tbody>
      </table>
    </div>
    <div class="section">
      <h3>Buffer Clocks</h3>
      <table>
        <thead><tr><th>pod</th><th>facts</th></tr></thead>
        <tbody>${bufferRows.map(r=>`<tr><td class="mono">${r.pod}</td><td>${esc(r.facts)}</td></tr>`).join('')}</tbody>
      </table>
    </div>
    <div class="section">
      <h3>Observed Coordination Receivers</h3>
      <table>
        <thead><tr><th>receiver</th><th>count</th></tr></thead>
        <tbody>${relayRows.map(r=>`<tr><td class="mono">${r.node}</td><td>${r.count}</td></tr>`).join('')}</tbody>
      </table>
    </div>
    <div class="section">
      <h3>Recent Structural Events</h3>
      <table>
        <thead><tr><th>date</th><th>pod</th><th>event</th></tr></thead>
        <tbody>${events.map(e=>`<tr><td class="mono">${(e.timestamp||'').slice(0,10)}</td><td class="mono">${e.pod}</td><td>${esc(e.event||'')} — ${esc(e.detail||'')}</td></tr>`).join('')}</tbody>
      </table>
    </div>`;
}

function graphNodesFor(mode){
  const pods=mapData.pods||{};
  const depended=Object.fromEntries(((analysis.depended_upon)||[]).map(r=>[r.pod,r.dependent_count]));
  const impacts=Object.fromEntries(((analysis.failure_impact_ranking)||[]).map(r=>[r.pod,r.total_offline]));
  const coord=(analysis.coordination_summary||{});
  const podTraffic={};
  Object.keys(pods).forEach(id=>podTraffic[id]=0);
  (coord.senders||[]).forEach(r=>{ const root=actorRoot(r.node); if(root) podTraffic[root]=(podTraffic[root]||0)+r.count; });
  (coord.receivers||[]).forEach(r=>{ const root=actorRoot(r.node); if(root) podTraffic[root]=(podTraffic[root]||0)+r.count; });
  return Object.keys(pods).map(id=>{
    const klass=classifyNode(id); const color=nodeColors(klass);
    const val=mode==='material' ? ((depended[id]||0)+1) : mode==='cascade' ? ((impacts[id]||1)+1) : ((podTraffic[id]||0)+1);
    return {id,label:id,value:val,color,shape:'dot',font:{color:'#1f1b18',face:'Iowan Old Style'},borderWidth:1.2,title:(pods[id].info||{}).role||id};
  });
}

function materialEdges(){
  const edges=[];
  Object.entries(mapData.pods||{}).forEach(([id,p])=>{ (p.dependencies||[]).forEach(d=>{ const crit=(d.criticality||'').toLowerCase(); edges.push({from:id,to:d.pod_id,arrows:'to',width:crit==='high'?2.6:crit==='medium'?1.7:1.1,color:{color:crit==='high'?'#8a3939':crit==='medium'?'#a87926':'#b5ada1'},title:`${d.resource||''} (${crit||'n/a'})`}); }); });
  return edges;
}

function cascadeEdges(){
  const edges=[];
  Object.entries(mapData.pods||{}).forEach(([id,p])=>{ (p.dependencies||[]).forEach(d=>{ const crit=(d.criticality||'').toLowerCase(); const res=(d.resource||'').toLowerCase(); if(crit==='high' && !['oversight','approval','directive','administrative','admin','planning','threat','assessment','report','coordination','monitoring','surveillance','authorization','policy','scheduling','management','logistics'].some(k=>res.includes(k))){ edges.push({from:id,to:d.pod_id,arrows:'to',width:2.8,color:{color:'#8a3939'},title:d.resource||''}); } }); });
  return edges;
}

function coordinationEdges(){
  const counts={};
  Object.entries(mapData.pods||{}).forEach(([pid,p])=>{ (p.comms||[]).forEach(m=>{ const from=actorRoot(m.from)||pid; const to=actorRoot(m.to); if(!from || !to || from===to) return; const key=`${from}__${to}`; counts[key]=(counts[key]||0)+1; }); });
  return Object.entries(counts).map(([k,count])=>{ const [from,to]=k.split('__'); return {from,to,arrows:'to',width:1+count*.8,color:{color:'#2c5d8a'},title:`${count} observed message${count===1?'':'s'}`}; });
}

function buildGraphData(mode){
  const nodes=graphNodesFor(mode);
  const edges=mode==='material'?materialEdges():mode==='cascade'?cascadeEdges():coordinationEdges();
  return {nodes,edges};
}

function renderGraph(){
  if(!mapData || !analysis) return;
  const wrap=$('graph-wrap'); wrap.classList.remove('empty'); wrap.innerHTML='<div id="graph"></div><div class="overlay" id="overlay"></div><div class="legend" id="legend"></div><div class="nodecard" id="nodecard"></div>';
  overlay=$('overlay'); legend=$('legend'); nodeCard=$('nodecard');
  const data=buildGraphData(graphMode); baseGraph=data;
  nodesDS=new vis.DataSet(data.nodes); edgesDS=new vis.DataSet(data.edges);
  network=new vis.Network($('graph'), {nodes:nodesDS, edges:edgesDS}, {
    autoResize:true,
    nodes:{shape:'dot',scaling:{min:10,max:34}},
    edges:{smooth:false,selectionWidth:0},
    interaction:{hover:true,multiselect:false},
    physics:{stabilization:{iterations:180}, barnesHut:{springLength:140,gravitationalConstant:-6000}},
    layout:{improvedLayout:true}
  });
  legend.innerHTML=MODE_META[graphMode].legend;
  $('graphTitle').textContent=MODE_META[graphMode].title;
  network.on('click',params=>{ if(injectMode && graphMode!=='coordination'){ if(params.nodes.length){ const id=params.nodes[0]; injected.has(id)?injected.delete(id):injected.add(id); renderInjection(); } return; } params.nodes.length?showNode(params.nodes[0]):clearNodeSelection(); });
  if(injectMode && graphMode!=='coordination') renderInjection();
}

function showNode(id){
  const p=mapData&&mapData.pods&&mapData.pods[id]; if(!p||!nodeCard) return;
  const info=p.info||{};
  const deps=(p.dependencies||[]).map(d=>`<li><span class="mono">${d.pod_id}</span><span class="crit ${(d.criticality||'').toLowerCase()}">${d.criticality||''}</span> ${esc(d.resource||'')}</li>`).join('') || '<li class="none">none</li>';
  const sup=(p.supplies||[]).map(s=>`<li><span class="mono">${s.pod_id}</span> ${esc(s.resource||'')}</li>`).join('') || '<li class="none">none</li>';
  const specs=Object.entries(info.metadata||{}).map(([k,v])=>`<li>${esc(k)}: <span class="mono">${esc(Array.isArray(v)?v.join(', '):String(v))}</span></li>`).join('') || '<li class="none">none</li>';
  nodeCard.innerHTML=`<button class="close" id="cardclose">×</button><h3>${esc(info.name||id)} <span>${id}</span></h3><div class="role">${esc(info.role||'')}</div><div class="meta">population ${info.population??'—'} · status ${esc((p.status||{}).status||'—')}</div><h4>Depends on</h4><ul>${deps}</ul><h4>Supplies</h4><ul>${sup}</ul><h4>Metadata</h4><ul>${specs}</ul>`;
  nodeCard.style.display='block'; $('cardclose').onclick=clearNodeSelection; highlightNeighbors(id);
}

function highlightNeighbors(id){
  if(!network||!nodesDS) return;
  const keep=new Set(network.getConnectedNodes(id)); keep.add(id);
  nodesDS.getIds().forEach(nid=>nodesDS.update({id:nid, opacity:keep.has(nid)?1:0.18}));
  network.selectNodes([id]);
}

function clearNodeSelection(){ if(nodeCard) nodeCard.style.display='none'; if(nodesDS) nodesDS.getIds().forEach(nid=>nodesDS.update({id:nid, opacity:1, shadow:false})); if(network) network.unselectAll(); }

function highlightStory(){
  if(!nodesDS) return;
  clearNodeSelection();
  const core=new Set((analysis.topology_summary||{}).core_cycle||[]);
  const ids=nodesDS.getIds();
  ids.forEach(id=>{ const n=nodesDS.get(id); const isCore=core.has(id); const isRelay=id==='artemis'; const isIsland=islandPods().includes(id); nodesDS.update({id,borderWidth:isCore||isRelay||isIsland?3.5:1.2,shadow:isCore||isRelay||isIsland?{enabled:true,color:isCore?'rgba(169,71,27,.28)':isRelay?'rgba(44,93,138,.22)':'rgba(56,116,90,.22)',size:18}:false,opacity:isCore||isRelay||isIsland?1:0.28,color:n.color}); });
  overlay.innerHTML=`<b>Structure:</b> the <span class="core">${fmtList((analysis.topology_summary||{}).core_cycle||[])}</span> core shares one blast radius; <span class="relay">artemis</span> absorbs the observed administrative traffic; <span class="island">${fmtList(islandPods())}</span> sit outside the main hard-cascade component.`;
  overlay.style.display='block';
  try{ network.fit({nodes:[...core,'artemis',...islandPods()], animation:{duration:500,easingFunction:'easeInOutQuad'}}); }catch(_){ }
}

function resetGraphStyles(){
  if(!baseGraph || !nodesDS) return;
  overlay.style.display='none';
  baseGraph.nodes.forEach(n=>nodesDS.update({...n, opacity:1, shadow:false, borderWidth:1.2}));
}

function simulateFailure(seed){
  const pods=mapData.pods, offline=new Set(seed); let changed=true;
  const adminKW=['oversight','approval','directive','administrative','admin','planning','threat','assessment','report','coordination','monitoring','surveillance','authorization','policy','scheduling','management','logistics'];
  const isMaterial=r=>{ r=(r||'').toLowerCase(); return !adminKW.some(k=>r.includes(k)); };
  while(changed){ changed=false; for(const id in pods){ if(offline.has(id)) continue; for(const d of (pods[id].dependencies||[])){ if(offline.has(d.pod_id) && (d.criticality||'').toLowerCase()==='high' && isMaterial(d.resource)){ offline.add(id); changed=true; break; } } } }
  return offline;
}

function renderInjection(){
  if(graphMode==='coordination') return;
  resetGraphStyles();
  const offline=simulateFailure([...injected]);
  nodesDS.getIds().forEach(id=>{ let color=nodeColors(classifyNode(id)); if(injected.has(id)) color={background:'#d48778',border:'#7d2222'}; else if(offline.has(id)) color={background:'#e5b27f',border:'#a4631c'}; else color={background:'#b9c9bd',border:'#38745a'}; nodesDS.update({id,color,opacity:1,borderWidth:injected.has(id)?3.8:1.8,shadow:false}); });
  const totalPop=Object.values(mapData.pods).reduce((s,p)=>s+((p.info||{}).population||0),0); const offPop=[...offline].reduce((s,id)=>s+((mapData.pods[id].info||{}).population||0),0); const cascaded=[...offline].filter(id=>!injected.has(id));
  overlay.innerHTML=injected.size===0 ? '<b>Failure injection:</b> click one or more pods to simulate a hard material outage. This view is local only; it does not touch the live colony.' : `<b>Failure injection:</b> failed <span class="core">${[...injected].join(', ')}</span>. Offline <b>${offline.size}/12 pods</b> and <b>${offPop}/${totalPop} residents</b>. ${cascaded.length?`Cascade: ${cascaded.join(', ')}.`:''}`;
  overlay.style.display='block';
}

function exitInjection(){ injectMode=false; injected.clear(); $('btnInject').classList.remove('active'); resetGraphStyles(); }

$('btnInject').onclick=()=>{ if(!mapData || graphMode==='coordination') return; if(injectMode){ exitInjection(); return; } injectMode=true; $('btnInject').classList.add('active'); clearNodeSelection(); renderInjection(); };
$('btnStory').onclick=()=>{ if(!analysis) return; if(injectMode) exitInjection(); highlightStory(); };
$('btnMap').onclick=()=>fetch('/api/map',{method:'POST'});
$('btnReport').onclick=()=>fetch('/api/report',{method:'POST'});
$('modeMaterial').onclick=()=>switchMode('material'); $('modeCascade').onclick=()=>switchMode('cascade'); $('modeCoord').onclick=()=>switchMode('coordination');
function switchMode(mode){ graphMode=mode; document.querySelectorAll('.graphmodes .mode').forEach(b=>b.classList.toggle('active', b.id===`mode${mode[0].toUpperCase()+mode.slice(1)}`)); if(injectMode && mode==='coordination') exitInjection(); renderGraph(); }

document.querySelectorAll('.modes .mode').forEach(btn=>btn.onclick=()=>{ currentTab=btn.dataset.tab; document.querySelectorAll('.modes .mode').forEach(b=>b.classList.toggle('active', b===btn)); document.querySelectorAll('.tabpane').forEach(p=>p.classList.remove('active')); if(currentTab==='chat'){ $('chatpane').classList.add('active'); $('chatpane').style.display='flex'; $('chatinput').focus(); } else { $('chatpane').style.display='none'; $(currentTab).classList.add('active'); } if(currentTab==='report' && reportText){ $('report').scrollTop=0; } });

const clog=()=>$('chatlog');
function addMsg(cls,html){ const d=document.createElement('div'); d.className='msg '+cls; d.innerHTML=html; clog().appendChild(d); clog().scrollTop=clog().scrollHeight; return d; }
function addTool(name,input){ const d=document.createElement('div'); d.className='toolchip'; d.innerHTML='<b>'+esc(name)+'</b>('+esc(JSON.stringify(input||{}))+')'; clog().appendChild(d); clog().scrollTop=clog().scrollHeight; return d; }
async function sendChat(){ const inp=$('chatinput'), btn=$('chatsend'); const msg=inp.value.trim(); if(!msg) return; const hint=clog().querySelector('.chathint'); if(hint) hint.remove(); inp.value=''; inp.disabled=true; btn.disabled=true; addMsg('user',esc(msg)); let think=addMsg('bot','<i style="color:#6e655c">thinking…</i>'), lastTool=null; try{ const res=await fetch('/api/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:msg,history:chatHistory})}); const reader=res.body.getReader(), dec=new TextDecoder(); let buf=''; while(true){ const {value,done}=await reader.read(); if(done) break; buf+=dec.decode(value,{stream:true}); let nl; while((nl=buf.indexOf('\n'))>=0){ const line=buf.slice(0,nl).trim(); buf=buf.slice(nl+1); if(!line) continue; const e=JSON.parse(line); if(think){ think.remove(); think=null; } if(e.type==='tool_call'){ lastTool=addTool(e.name,e.input); } else if(e.type==='tool_result'&&lastTool){ const r=document.createElement('span'); r.className='res'; r.textContent='→ '+e.preview; lastTool.appendChild(r); clog().scrollTop=clog().scrollHeight; } else if(e.type==='answer'){ addMsg('bot',marked.parse(e.content||'(no answer)')); chatHistory.push({role:'user',content:msg}); chatHistory.push({role:'assistant',content:e.content||''}); } else if(e.type==='error'){ addMsg('bot','<span style="color:#7d2222">'+esc(e.error)+'</span>'); } } } }catch(err){ if(think) think.remove(); addMsg('bot','<span style="color:#7d2222">'+esc(String(err))+'</span>'); } inp.disabled=false; btn.disabled=false; inp.focus(); }
$('chatsend').onclick=sendChat; $('chatinput').addEventListener('keydown',e=>{ if(e.key==='Enter') sendChat(); });

const es=new EventSource('/events');
es.onmessage=e=>{ try{ applyState(JSON.parse(e.data)); } catch(_){} };
es.onerror=()=>setConn(false);
fetch('/api/gateway').then(r=>r.ok?r.json():null).then(g=>{ if(g&&g.colony) $('colony').textContent=`${g.colony} · population ${g.population||'?'}`; }).catch(()=>{});
</script>
</body>
</html>
"""


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()
    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port)
