# /// script
# requires-python = ">=3.10"
# dependencies = ["fastapi", "uvicorn", "httpx", "networkx", "anthropic", "openai"]
# ///
"""Selene Colony Monitor — a standalone, read-only dashboard.

This lives OUTSIDE the evaluated project-selene repo and touches none of it. It
only consumes the rover's already-published host ports:
  - rover    http://localhost:8080   (/health, POST /map, GET /get-map, POST /report, GET /get-report)
  - gateway  http://localhost:3000

It polls those poll-only endpoints server-side and re-emits a live Server-Sent
Events stream to the browser, plus proxies the artifacts (avoids CORS without
modifying the rover). Run:  uv run dashboard.py   then open http://localhost:8090
"""
import argparse
import asyncio
import json
import os
import pathlib
import sys

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse

ROVER = os.environ.get("ROVER_URL", "http://localhost:8080")
GATEWAY = os.environ.get("GATEWAY_URL", "http://localhost:3000")

# Reuse the deliverable's engine + tools READ-ONLY (no files changed, runs in this
# process) so the chat exercises the exact same tools the reporting agent uses.
ROVER_CODE = os.environ.get("ROVER_CODE_PATH") or str(
    pathlib.Path(__file__).resolve().parent.parent / "project-selene" / "rover")
AGENT_OK, AGENT_ERR = False, None
try:
    if ROVER_CODE not in sys.path:
        sys.path.insert(0, ROVER_CODE)
    from agent.engine import ColonyEngine
    from agent import tools as agent_tools
    AGENT_OK = True
except Exception as e:  # dashboard still runs without chat if the repo isn't found
    ColonyEngine, agent_tools, AGENT_ERR = None, None, f"{type(e).__name__}: {e}"

app = FastAPI()


async def _probe(client: httpx.AsyncClient) -> dict:
    """Poll the rover's job endpoints and derive a single colony phase."""
    state = {"map_http": None, "report_http": None, "map_started": None,
             "report_started": None, "online": True}
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
    """SSE stream: emit a full event whenever the colony phase signature changes."""
    async def gen():
        last_sig = None
        async with httpx.AsyncClient(timeout=4.0) as client:
            while True:
                st = await _probe(client)
                sig = (st["phase"], st["map_http"], st["report_http"],
                       st["map_started"], st["report_started"])
                if sig != last_sig:
                    yield f"data: {json.dumps(st)}\n\n"
                    last_sig = sig
                else:
                    yield ": heartbeat\n\n"
                await asyncio.sleep(1.5)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


async def _proxy(method: str, url: str) -> Response:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.request(method, url)
        return Response(content=r.content, status_code=r.status_code,
                        media_type=r.headers.get("content-type", "application/json"))
    except (httpx.TransportError, httpx.TimeoutException) as e:
        return Response(content=json.dumps({"error": f"rover unreachable: {e}"}),
                        status_code=503, media_type="application/json")


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


def _llm_key():
    if os.environ.get("LLM_API_KEY"):
        return os.environ["LLM_API_KEY"]
    envp = pathlib.Path(ROVER_CODE).parent / ".env"  # fall back to the project .env
    vals = {}
    if envp.exists():
        for line in envp.read_text().splitlines():
            line = line.strip()
            for name in ("LLM_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY"):
                if line.startswith(name + "="):
                    vals[name] = line.split("=", 1)[1].strip()
    # Prefer by name (LLM_API_KEY wins), NOT by file order.
    for name in ("LLM_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY"):
        if vals.get(name):
            return vals[name]
    return None


_MAP_CACHE = {"doc": None}


async def _load_engine():
    """Build a ColonyEngine from the latest map.json (cached so chat survives rover restarts)."""
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(ROVER + "/get-map")
        if r.status_code == 200:
            _MAP_CACHE["doc"] = r.json()
    except (httpx.TransportError, httpx.TimeoutException):
        pass
    return ColonyEngine(_MAP_CACHE["doc"]) if _MAP_CACHE["doc"] else None


CHAT_SYSTEM = (
    "You are the Selene colony infrastructure analyst. Answer the user's questions about the colony by "
    "calling the available tools to gather evidence, then giving a concise, specific answer grounded in "
    "that data — cite pod ids and numbers. Prefer tools over assumptions. Keep answers short."
)


def _chat_anthropic(key, engine, history, message):
    from anthropic import Anthropic
    client = Anthropic(api_key=key)
    model = os.environ.get("LLM_MODEL", "claude-sonnet-4-6")
    spec = [{"name": t["name"], "description": t["description"], "input_schema": t["input_schema"]}
            for t in agent_tools.TOOL_SPECS]
    messages = [{"role": m["role"], "content": m["content"]} for m in history]
    messages.append({"role": "user", "content": message})
    for _ in range(12):
        resp = client.messages.create(model=model, max_tokens=2000, system=CHAT_SYSTEM, tools=spec, messages=messages)
        messages.append({"role": "assistant", "content": resp.content})
        if resp.stop_reason != "tool_use":
            yield {"type": "answer", "content": "".join(b.text for b in resp.content if b.type == "text")}
            return
        results = []
        for b in resp.content:
            if b.type == "tool_use":
                yield {"type": "tool_call", "name": b.name, "input": b.input}
                out = json.dumps(agent_tools.dispatch(engine, b.name, b.input), default=str)
                yield {"type": "tool_result", "name": b.name, "preview": out[:400]}
                results.append({"type": "tool_result", "tool_use_id": b.id, "content": out})
        messages.append({"role": "user", "content": results})
    yield {"type": "error", "error": "max turns reached"}


def _chat_openai(key, engine, history, message):
    from openai import OpenAI
    client = OpenAI(api_key=key)
    model = os.environ.get("LLM_MODEL", "gpt-4o")
    spec = [{"type": "function", "function": {"name": t["name"], "description": t["description"], "parameters": t["input_schema"]}}
            for t in agent_tools.TOOL_SPECS]
    messages = [{"role": "system", "content": CHAT_SYSTEM}]
    messages += [{"role": m["role"], "content": m["content"]} for m in history]
    messages.append({"role": "user", "content": message})
    for _ in range(12):
        resp = client.chat.completions.create(model=model, messages=messages, tools=spec)
        m = resp.choices[0].message
        if not m.tool_calls:
            yield {"type": "answer", "content": m.content or ""}
            return
        messages.append({"role": "assistant", "content": m.content, "tool_calls": m.tool_calls})
        for tc in m.tool_calls:
            args = json.loads(tc.function.arguments or "{}")
            yield {"type": "tool_call", "name": tc.function.name, "input": args}
            out = json.dumps(agent_tools.dispatch(engine, tc.function.name, args), default=str)
            yield {"type": "tool_result", "name": tc.function.name, "preview": out[:400]}
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": out})
    yield {"type": "error", "error": "max turns reached"}


def _chat_stream(engine, history, message):
    key = _llm_key()
    if not key:
        yield {"type": "error", "error": "LLM_API_KEY not set (and none found in project .env)"}
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
        for ev in _chat_stream(engine, body.get("history", []), body.get("message", "")):
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
  :root{--bg:#0a0e14;--panel:#121822;--line:#1f2a3a;--txt:#c9d4e3;--dim:#6b7d95;--accent:#4da3ff;}
  *{box-sizing:border-box;}
  body{margin:0;font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;background:var(--bg);color:var(--txt);}
  header{display:flex;align-items:center;gap:16px;padding:14px 20px;border-bottom:1px solid var(--line);background:#0c1118;}
  header h1{font-size:15px;letter-spacing:3px;margin:0;font-weight:600;}
  .colony{color:var(--dim);font-size:12px;}
  .conn{margin-left:auto;display:flex;align-items:center;gap:7px;font-size:12px;color:var(--dim);}
  .dot{width:9px;height:9px;border-radius:50%;background:#3a4a5e;}
  .dot.live{background:#37d67a;box-shadow:0 0 8px #37d67a;}
  .banner{display:flex;align-items:center;gap:18px;padding:18px 20px;border-bottom:1px solid var(--line);}
  .phase{font-size:22px;font-weight:700;letter-spacing:1px;}
  .elapsed{color:var(--dim);font-variant-numeric:tabular-nums;}
  .spin{width:16px;height:16px;border:2px solid #2a3a4e;border-top-color:var(--accent);border-radius:50%;animation:s .8s linear infinite;display:none;}
  @keyframes s{to{transform:rotate(360deg)}}
  .controls{margin-left:auto;display:flex;gap:10px;}
  button{font:inherit;background:#17202e;color:var(--txt);border:1px solid var(--line);padding:8px 16px;border-radius:6px;cursor:pointer;}
  button:hover:not(:disabled){border-color:var(--accent);color:#fff;}
  button:disabled{opacity:.4;cursor:not-allowed;}
  button.active{background:#5a1414;border-color:#ff5d5d;color:#ffd9d9;}
  .injectinfo{position:absolute;top:10px;left:10px;font-size:12px;color:var(--txt);background:rgba(20,10,10,.92);padding:9px 12px;border:1px solid #ff5d5d;border-radius:6px;max-width:62%;line-height:1.45;}
  .injectinfo .bad{color:#ff8c6b;font-weight:700;}
  .injectinfo .ok{color:#37d67a;}
  .injectinfo .dim{color:var(--dim);}
  main{display:grid;grid-template-columns:1fr 1fr;grid-template-rows:minmax(0,1fr);gap:1px;background:var(--line);height:calc(100vh - 118px);}
  .panel{background:var(--bg);display:flex;flex-direction:column;min-width:0;min-height:0;overflow:hidden;}
  .panel h2{font-size:11px;letter-spacing:2px;color:var(--dim);margin:0;padding:10px 16px;border-bottom:1px solid var(--line);text-transform:uppercase;}
  .tabs{display:flex;border-bottom:1px solid var(--line);}
  .tab{background:none;border:none;border-bottom:2px solid transparent;border-radius:0;color:var(--dim);font-size:11px;letter-spacing:2px;text-transform:uppercase;padding:10px 16px;}
  .tab:hover:not(:disabled){color:var(--txt);}
  .tab.active{color:#fff;border-bottom-color:var(--accent);}
  .tabpane{flex:1;min-height:0;}
  #chatpane{display:flex;flex-direction:column;}
  #chatlog{flex:1;overflow:auto;padding:14px 16px;display:flex;flex-direction:column;gap:9px;}
  .chathint{color:var(--dim);font-size:12px;line-height:1.5;}
  .msg{max-width:90%;padding:7px 11px;border-radius:8px;font-size:13px;line-height:1.5;}
  .msg.user{align-self:flex-end;background:#17314e;color:#dbe9ff;}
  .msg.bot{align-self:flex-start;background:#141d29;border:1px solid var(--line);}
  .msg.bot p:first-child{margin-top:0;} .msg.bot p:last-child{margin-bottom:0;}
  .toolchip{align-self:flex-start;font-size:11px;font-family:ui-monospace,Menlo,monospace;color:#c39bff;background:#1a1426;border:1px solid #2e2440;border-radius:6px;padding:5px 9px;max-width:92%;}
  .toolchip .res{color:var(--dim);display:block;margin-top:3px;white-space:pre-wrap;word-break:break-word;}
  .chatbar{display:flex;gap:8px;padding:10px;border-top:1px solid var(--line);}
  .chatbar input{flex:1;background:#0c1118;border:1px solid var(--line);color:var(--txt);border-radius:6px;padding:8px 10px;font:inherit;}
  .chatbar input:focus{outline:none;border-color:var(--accent);}
  #graph{flex:1;}
  #report{flex:1;overflow:auto;padding:8px 26px 60px;}
  #report.empty,#graph-wrap.empty{display:flex;align-items:center;justify-content:center;color:var(--dim);}
  #graph-wrap{flex:1;position:relative;min-height:0;}
  .legend{position:absolute;bottom:10px;left:10px;font-size:11px;color:var(--dim);background:rgba(12,17,24,.85);padding:8px 10px;border:1px solid var(--line);border-radius:6px;}
  .legend b{color:#ff5d5d;}
  .nodecard{position:absolute;top:10px;right:10px;width:290px;max-height:calc(100% - 20px);overflow:auto;background:rgba(14,21,30,.97);border:1px solid var(--accent);border-radius:8px;padding:12px 14px;font-size:12px;box-shadow:0 6px 24px rgba(0,0,0,.5);}
  .nodecard .cardclose{position:absolute;top:6px;right:8px;background:none;border:none;color:var(--dim);font-size:18px;padding:0;cursor:pointer;}
  .nodecard h3{margin:0 0 2px;font-size:15px;color:#fff;}
  .nodecard h3 span{color:var(--dim);font-size:11px;font-weight:400;}
  .nodecard .role{color:#9ec5ff;font-size:12px;margin-bottom:4px;}
  .nodecard .cmeta{color:var(--dim);font-size:11px;margin-bottom:8px;}
  .nodecard h4{margin:10px 0 3px;font-size:10px;letter-spacing:1.5px;color:var(--dim);text-transform:uppercase;}
  .nodecard ul{margin:0;padding-left:2px;list-style:none;}
  .nodecard li{padding:1px 0;}
  .nodecard li i{color:var(--dim);font-style:normal;}
  .nodecard li.none{color:var(--dim);}
  .nodecard .specs li{color:#aebfd2;}
  .crit{font-size:9px;padding:0 5px;border-radius:3px;font-weight:700;}
  .crit.high{background:#ff5d5d;color:#1a0a0a;} .crit.medium{background:#ffb454;color:#241400;} .crit.low{background:#2a3a4e;color:#9ec5ff;}
  /* markdown */
  #report h1{font-size:22px;border-bottom:1px solid var(--line);padding-bottom:8px;}
  #report h2{font-size:17px;color:#9ec5ff;margin-top:28px;}
  #report h3{font-size:14px;color:#cdd9e8;}
  #report table{border-collapse:collapse;width:100%;font-size:12.5px;margin:12px 0;}
  #report th,#report td{border:1px solid var(--line);padding:5px 9px;text-align:left;}
  #report th{background:#141d29;}
  #report code{background:#141d29;padding:1px 5px;border-radius:4px;font-size:12px;}
  #report blockquote{border-left:3px solid var(--accent);margin:10px 0;padding:2px 14px;color:#aebfd2;background:#0e151e;}
  #report a{color:var(--accent);}
  .ph-idle{color:#6b7d95}.ph-mapping{color:#ffb454}.ph-mapped{color:#9ec5ff}.ph-reporting{color:#c39bff}.ph-report_ready{color:#37d67a}.ph-error{color:#ff5d5d}.ph-offline{color:#ff5d5d}
</style>
</head>
<body>
<header>
  <h1>◐ SELENE COLONY MONITOR</h1>
  <span class="colony" id="colony">connecting…</span>
  <span class="conn"><span class="dot" id="cdot"></span><span id="cstat">stream offline</span></span>
</header>
<div class="banner">
  <div class="spin" id="spin"></div>
  <div class="phase ph-idle" id="phase">—</div>
  <div class="elapsed" id="elapsed"></div>
  <div class="controls">
    <button id="btnMap">▶ Run Mapping</button>
    <button id="btnReport">▶ Run Report</button>
    <button id="btnKey" disabled>⚑ Key Findings</button>
    <button id="btnInject" disabled>💥 Inject Failure</button>
  </div>
</div>
<main>
  <div class="panel">
    <h2>Dependency Graph <span id="gmeta" style="color:var(--dim)"></span></h2>
    <div id="graph-wrap" class="empty"><span id="gph">no map yet — run mapping</span></div>
  </div>
  <div class="panel">
    <div class="tabs">
      <button class="tab active" data-tab="report">Latest Report</button>
      <button class="tab" data-tab="chat">💬 Chat with Agent</button>
    </div>
    <div id="report" class="tabpane empty">no report yet — run report</div>
    <div id="chatpane" class="tabpane" style="display:none">
      <div id="chatlog"><div class="chathint">Ask the agent about the colony — it answers by calling the same tools the reporting agent uses. Try: <i>"What happens if Helios fails?"</i> or <i>"Which supply/dependency mismatches matter?"</i></div></div>
      <div class="chatbar"><input id="chatinput" placeholder="Ask the agent…" autocomplete="off"/><button id="chatsend">Send</button></div>
    </div>
  </div>
</main>
<script>
const PHASE_LABEL={idle:"IDLE",mapping:"MAPPING…",mapped:"MAP READY",reporting:"REPORTING…",report_ready:"REPORT READY",error:"ERROR",offline:"ROVER OFFLINE"};
let mapLoaded=false, reportLoaded=false, timer=null, network=null, nodesDS=null;
let keyPlayers=[], keyHeading=null, mapData=null, card=null;
let injectMode=false, injected=new Set(), baseNodes=null, injectInfo=null;

function setConn(live){document.getElementById('cdot').classList.toggle('live',live);
  document.getElementById('cstat').textContent=live?'live':'stream offline';}

function tickElapsed(startISO){
  clearInterval(timer);
  const el=document.getElementById('elapsed');
  if(!startISO){el.textContent='';return;}
  const start=new Date(startISO).getTime();
  const upd=()=>{const s=Math.max(0,Math.floor((Date.now()-start)/1000));
    el.textContent=String(Math.floor(s/60)).padStart(2,'0')+':'+String(s%60).padStart(2,'0')+' elapsed';};
  upd();timer=setInterval(upd,1000);
}

function applyState(st){
  setConn(true);
  const ph=document.getElementById('phase');
  ph.textContent=PHASE_LABEL[st.phase]||st.phase;
  ph.className='phase ph-'+st.phase;
  const running=(st.phase==='mapping'||st.phase==='reporting');
  document.getElementById('spin').style.display=running?'block':'none';
  tickElapsed(st.phase==='mapping'?st.map_started:(st.phase==='reporting'?st.report_started:null));
  document.getElementById('btnMap').disabled=running;
  document.getElementById('btnReport').disabled=running||!(st.map_http===200);
  document.getElementById('btnKey').disabled=!(st.report_http===200);
  document.getElementById('btnInject').disabled=!(st.map_http===200);
  // reset load flags when a fresh run begins
  if(st.phase==='mapping'){mapLoaded=false;reportLoaded=false;}
  if(st.phase==='reporting'){reportLoaded=false;}
  if(st.map_http===200 && !mapLoaded){mapLoaded=true;loadMap();}
  if(st.report_http===200 && !reportLoaded){reportLoaded=true;loadReport();}
}

const CRIT={high:'#ff5d5d',medium:'#ffb454',low:'#3a4a5e'};
async function loadMap(){
  const r=await fetch('/api/map'); if(r.status!==200)return;
  const data=await r.json(); mapData=data; const pods=data.pods||{};
  const inDeg={}; Object.keys(pods).forEach(p=>inDeg[p]=0);
  Object.values(pods).forEach(p=>(p.dependencies||[]).forEach(d=>{if(d.pod_id in inDeg)inDeg[d.pod_id]++;}));
  const maxIn=Math.max(1,...Object.values(inDeg));
  const nodes=Object.entries(pods).map(([id,p])=>{
    const deg=inDeg[id], t=deg/maxIn;
    const col=`rgb(${Math.round(40+t*215)},${Math.round(80-t*40)},${Math.round(95-t*55)})`;
    return {id,label:id+(deg>=Math.ceil(maxIn*0.8)?' ⚠':''),title:(p.info&&p.info.role)||id,
      value:deg+1,color:{background:col,border:'#0a0e14'},font:{color:'#e8eef6'}};
  });
  const edges=[];
  Object.entries(pods).forEach(([id,p])=>(p.dependencies||[]).forEach(d=>{
    edges.push({from:id,to:d.pod_id,arrows:'to',color:{color:CRIT[(d.criticality||'').toLowerCase()]||'#2a3a4e'},
      width:(d.criticality==='high')?2.5:1,title:d.resource||''});}));
  const wrap=document.getElementById('graph-wrap'); wrap.classList.remove('empty'); wrap.innerHTML='';
  const cv=document.createElement('div'); cv.id='graph'; cv.style.height='100%'; wrap.appendChild(cv);
  const lg=document.createElement('div'); lg.className='legend';
  lg.innerHTML='node size/redness = how depended-upon · <b>⚠</b> = top dependency hub<br>edge color = criticality (red=high, orange=med) · click a pod for details';
  wrap.appendChild(lg);
  card=document.createElement('div'); card.className='nodecard'; card.style.display='none'; wrap.appendChild(card);
  injectInfo=document.createElement('div'); injectInfo.className='injectinfo'; injectInfo.style.display='none'; wrap.appendChild(injectInfo);
  baseNodes=nodes.map(n=>({id:n.id,label:n.label,color:n.color}));
  nodesDS=new vis.DataSet(nodes);
  network=new vis.Network(cv,{nodes:nodesDS,edges:new vis.DataSet(edges)},
    {physics:{stabilization:true,barnesHut:{springLength:140}},interaction:{hover:true},nodes:{shape:'dot',scaling:{min:12,max:42}}});
  network.on('click',params=>{
    if(injectMode){ if(params.nodes.length){const id=params.nodes[0]; injected.has(id)?injected.delete(id):injected.add(id); renderInjection();} return; }
    params.nodes.length?showNode(params.nodes[0]):clearNodeSelection();
  });
  document.getElementById('gmeta').textContent='· '+nodes.length+' pods';
}

async function loadReport(){
  const r=await fetch('/api/report'); if(r.status!==200)return;
  const md=await r.text(); const el=document.getElementById('report');
  el.classList.remove('empty'); el.innerHTML=marked.parse(md); el.scrollTop=0;
  keyPlayers=parseKeyPlayers(md);
  keyHeading=findKeyHeading();
}

// Key players = the pods tied for the largest individual blast radius, from the
// deterministic "Single-pod failure impact" appendix table.
function parseKeyPlayers(md){
  const idx=md.indexOf('Single-pod failure impact'); if(idx<0)return [];
  let tail=md.slice(idx); const end=tail.indexOf('\n## ',10); if(end>0)tail=tail.slice(0,end);
  const rows=[];
  tail.split('\n').forEach(line=>{
    if(!line.trim().startsWith('|'))return;
    const c=line.split('|').map(s=>s.trim());   // c[1]=pod, c[3]=pods offline
    const off=parseInt(c[3],10);
    if(c[1] && !isNaN(off) && c[1].toLowerCase()!=='pod') rows.push([c[1],off]);
  });
  if(!rows.length)return [];
  const max=Math.max(...rows.map(r=>r[1]));
  return max>1 ? rows.filter(r=>r[1]===max).map(r=>r[0]) : [];
}

// Find the most relevant report heading to scroll to.
function findKeyHeading(){
  const hs=[...document.querySelectorAll('#report h1,#report h2,#report h3')];
  for(const kw of ['key player','single point','hidden single','blast rad','critical pod']){
    const h=hs.find(x=>x.textContent.toLowerCase().includes(kw)); if(h)return h;
  }
  return null;
}

function highlightKeyPlayers(){
  if(!network||!nodesDS)return;
  const ids=keyPlayers.filter(id=>nodesDS.get(id)); if(!ids.length)return;
  ids.forEach(id=>{const n=nodesDS.get(id);
    nodesDS.update({id,borderWidth:5,color:{background:n.color.background,border:'#ffd34d'},
      shadow:{enabled:true,color:'rgba(255,211,77,.75)',size:28}});});
  network.selectNodes(ids);
  try{network.fit({nodes:ids,animation:{duration:700,easingFunction:'easeInOutQuad'}});}catch(_){}
}

// --- node click: detail card + position highlight ---
function showNode(id){
  const p=mapData&&mapData.pods&&mapData.pods[id]; if(!p||!card)return;
  const info=p.info||{};
  const li=arr=>arr&&arr.length?arr:null;
  const deps=(li(p.dependencies)||[]).map(d=>`<li>→ <b>${d.pod_id}</b> <span class="crit ${(d.criticality||'').toLowerCase()}">${d.criticality||''}</span> <i>${d.resource||''}</i></li>`).join('')||'<li class="none">none</li>';
  const sup=(li(p.supplies)||[]).map(s=>`<li>→ <b>${s.pod_id}</b> <i>${s.resource||''}</i></li>`).join('')||'<li class="none">none</li>';
  const specs=Object.entries(info.metadata||{}).slice(0,6).map(([k,v])=>`<li>${k}: <b>${Array.isArray(v)?v.join(', '):v}</b></li>`).join('');
  const st=info.status||(p.status&&p.status.status)||'?';
  card.innerHTML=`<button class="cardclose" id="cardX">×</button>`+
    `<h3>${info.name||id} <span>${id}</span></h3>`+
    `<div class="role">${info.role||''}</div>`+
    `<div class="cmeta">pop ${info.population??'?'} · status ${st}</div>`+
    `<h4>Depends on</h4><ul>${deps}</ul>`+
    `<h4>Supplies</h4><ul>${sup}</ul>`+
    (specs?`<h4>Specs</h4><ul class="specs">${specs}</ul>`:'');
  card.style.display='block';
  document.getElementById('cardX').onclick=clearNodeSelection;
  highlightNeighbors(id);
}
function highlightNeighbors(id){
  if(!network||!nodesDS)return;
  const keep=new Set(network.getConnectedNodes(id)); keep.add(id);
  nodesDS.getIds().forEach(nid=>nodesDS.update({id:nid,opacity:keep.has(nid)?1:0.12}));
  network.selectNodes([id]);
}
function clearNodeSelection(){
  if(card)card.style.display='none';
  if(nodesDS)nodesDS.getIds().forEach(nid=>nodesDS.update({id:nid,opacity:1}));
  if(network)network.unselectAll();
}

// --- failure injection (client-side what-if; never touches the live colony) ---
const ADMIN_KW=['oversight','approval','directive','administrative','admin','planning','threat','assessment','report','coordination','monitoring','surveillance','authorization','policy','scheduling','management','logistics'];
const isMaterial=r=>{r=(r||'').toLowerCase();return !ADMIN_KW.some(k=>r.includes(k));};

// Mirror of engine.simulate_failure: cascade through high-criticality material deps to a fixpoint.
function simulateFailure(seed){
  const pods=mapData.pods, offline=new Set(seed); let changed=true;
  while(changed){ changed=false;
    for(const id in pods){ if(offline.has(id))continue;
      for(const d of (pods[id].dependencies||[])){
        if(offline.has(d.pod_id) && (d.criticality||'').toLowerCase()==='high' && isMaterial(d.resource)){ offline.add(id); changed=true; break; }
      }
    }
  }
  return offline;
}

function renderInjection(){
  const offline=simulateFailure([...injected]);
  nodesDS.getIds().forEach(id=>{
    let color;
    if(injected.has(id)) color={background:'#7a0000',border:'#ff2d2d'};           // directly failed
    else if(offline.has(id)) color={background:'#9a4a16',border:'#ff8c3b'};       // cascaded offline
    else color={background:'#1c5e36',border:'#37d67a'};                            // surviving
    nodesDS.update({id,color,opacity:1,borderWidth:injected.has(id)?5:2,shadow:false});
  });
  const totalPop=Object.values(mapData.pods).reduce((s,p)=>s+((p.info||{}).population||0),0);
  const offPop=[...offline].reduce((s,id)=>s+((mapData.pods[id].info||{}).population||0),0);
  const cascaded=[...offline].filter(id=>!injected.has(id));
  injectInfo.innerHTML = injected.size===0
    ? '💥 <b>INJECTION MODE</b> — click pods to fail them. <span class="ok">all systems nominal</span>'
    : `💥 <b>INJECTION</b> · failed: ${[...injected].join(', ')||'—'}<br>`+
      `<span class="bad">${offline.size}/${nodesDS.length} pods offline · ${offPop}/${totalPop} residents</span>`+
      (cascaded.length?`<br><span class="dim">cascaded: ${cascaded.join(', ')}</span>`:'');
  injectInfo.style.display='block';
}

function exitInjection(){
  injected.clear(); injectMode=false;
  document.getElementById('btnInject').classList.remove('active');
  if(injectInfo)injectInfo.style.display='none';
  if(nodesDS&&baseNodes)baseNodes.forEach(n=>nodesDS.update({id:n.id,color:n.color,opacity:1,borderWidth:1,shadow:false}));
}

document.getElementById('btnInject').onclick=()=>{
  if(injectMode){ exitInjection(); return; }
  injectMode=true; clearNodeSelection();
  document.getElementById('btnInject').classList.add('active');
  injected.clear(); renderInjection();
};

document.getElementById('btnMap').onclick=()=>fetch('/api/map',{method:'POST'});
document.getElementById('btnReport').onclick=()=>fetch('/api/report',{method:'POST'});
document.getElementById('btnKey').onclick=()=>{
  if(keyHeading)keyHeading.scrollIntoView({behavior:'smooth',block:'start'});
  highlightKeyPlayers();
};

// --- tabs ---
document.querySelectorAll('.tab').forEach(t=>t.onclick=()=>{
  document.querySelectorAll('.tab').forEach(x=>x.classList.remove('active'));
  t.classList.add('active');
  const isChat=t.dataset.tab==='chat';
  document.getElementById('report').style.display=isChat?'none':'';
  document.getElementById('chatpane').style.display=isChat?'flex':'none';
  if(isChat)document.getElementById('chatinput').focus();
});

// --- chat: streams the agent's tool-use loop ---
let chatHistory=[];
const esc=s=>(s||'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
const clog=()=>document.getElementById('chatlog');
function addMsg(cls,html){const d=document.createElement('div');d.className='msg '+cls;d.innerHTML=html;clog().appendChild(d);clog().scrollTop=clog().scrollHeight;return d;}
function addTool(name,input){const d=document.createElement('div');d.className='toolchip';d.innerHTML='🔧 <b>'+esc(name)+'</b>('+esc(JSON.stringify(input||{}))+')';clog().appendChild(d);clog().scrollTop=clog().scrollHeight;return d;}

async function sendChat(){
  const inp=document.getElementById('chatinput'), btn=document.getElementById('chatsend');
  const msg=inp.value.trim(); if(!msg)return;
  const hint=clog().querySelector('.chathint'); if(hint)hint.remove();
  inp.value=''; inp.disabled=true; btn.disabled=true;
  addMsg('user',esc(msg));
  let think=addMsg('bot','<i style="color:var(--dim)">…thinking</i>'), lastTool=null;
  try{
    const res=await fetch('/api/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:msg,history:chatHistory})});
    const reader=res.body.getReader(), dec=new TextDecoder(); let buf='';
    while(true){
      const {value,done}=await reader.read(); if(done)break;
      buf+=dec.decode(value,{stream:true}); let nl;
      while((nl=buf.indexOf('\n'))>=0){
        const line=buf.slice(0,nl).trim(); buf=buf.slice(nl+1); if(!line)continue;
        const e=JSON.parse(line);
        if(think){think.remove();think=null;}
        if(e.type==='tool_call'){lastTool=addTool(e.name,e.input);}
        else if(e.type==='tool_result'&&lastTool){const r=document.createElement('span');r.className='res';r.textContent='→ '+e.preview;lastTool.appendChild(r);clog().scrollTop=clog().scrollHeight;}
        else if(e.type==='answer'){addMsg('bot',marked.parse(e.content||'(no answer)'));chatHistory.push({role:'user',content:msg});chatHistory.push({role:'assistant',content:e.content||''});}
        else if(e.type==='error'){addMsg('bot','<span style="color:#ff6b6b">⚠ '+esc(e.error)+'</span>');}
      }
    }
  }catch(err){if(think)think.remove();addMsg('bot','<span style="color:#ff6b6b">⚠ '+esc(String(err))+'</span>');}
  inp.disabled=false; btn.disabled=false; inp.focus();
}
document.getElementById('chatsend').onclick=sendChat;
document.getElementById('chatinput').addEventListener('keydown',e=>{if(e.key==='Enter')sendChat();});

const es=new EventSource('/events');
es.onmessage=e=>{try{applyState(JSON.parse(e.data));}catch(_){}};
es.onerror=()=>setConn(false);

fetch('/api/gateway').then(r=>r.ok?r.json():null).then(g=>{
  if(g&&g.colony)document.getElementById('colony').textContent=g.colony+' · pop '+(g.population||'?');
}).catch(()=>{});
</script>
</body>
</html>"""


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)
