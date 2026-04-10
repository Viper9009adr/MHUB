# Runtime Contract Matrix (implemented)

## Contract files

- `docs/runtime_contract_matrix.md`
- `runtime/message_bus.py`
- `runtime/worker_runner.py`
- `runtime/cli_runtime.py`

## Data contracts

- `Req={rid:str,cmd:str,args:list[str]=[],meta:dict[str,str]={},timeout_ms:int=0}`
- `Evt={topic∈{run,sys},kind∈{queued,start,out,err,exit,cancel,done},rid:str,ts_ms:int,payload:dict[str,Any]={},err:Err|None=None}`
- `Err={type:str,msg:str,code:int=0,data:dict[str,Any]={}}`
- `RunResult={ok:bool,code:int,cancelled:bool=False,err:Err|None=None,events:list[Evt]=[]}`

## Runtime operations

- `submit(req)`: pre `rid/cmd` non-empty; runtime open; no duplicate `rid`. post emit `queued`, start background run task, return `rid`.
- `cancel(rid)`: pre run exists; runtime open. post set cancel event, emit `cancel`, return `True`.
- `await(rid,timeout_ms=0)`: pre run exists; runtime open. post return `RunResult`; timeout raises `RuntimeError("timeout")`; cancelled raises `CE`.
- `close()`: idempotent `True`; marks all active runs cancelled, waits pending tasks, emits `sys/done`, then closes bus.

## Prompt routing behavior note

- Explicit hub meta commands stay explicit (`status`/`report`/`location` intent resolution via `resolve_pre_llm_hub_intent`).
- Free-form prompts fall through to orchestrator flow in `HubService.AgentStream` (non-meta path).

## Known validation status

- `tests/integration/test_grpc_hub.py` (`3` failing)
- compile verification pending `python3 -m py_compile` requirement
