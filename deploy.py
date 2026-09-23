"""
deploy.py —— 支援 Parameter Context、度量遙測與 Fork/Join 並行的純函數式 NiFi 部署管線
"""
from __future__ import annotations

import logging
import os
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple
import nipyapi
import yaml

# 讀取目標 NiFi 位址
NIFI_BASE_URL = os.getenv("NIFI_HOST", "http://nifi:8080")
nipyapi.config.nifi_config.host = f"{NIFI_BASE_URL}/nifi-api"

logger = logging.getLogger("nifi_pipeline")

# =====================================================================
# 階段 1: 規格解析與圖論排版 (純函數, CPU-bound)
# =====================================================================
def analyze_spec(spec: dict) -> dict:
    proc_specs = spec.get("processors", [])
    conn_specs = spec.get("connections", [])
    parameters = spec.get("parameters", {})

    used_rels: dict[str, set[str]] = defaultdict(set)
    for c in conn_specs:
        for r in c.get("relationships", []):
            used_rels[c["source"]].add(r)

    return {
        "pipeline_name": spec.get("pipeline_name", "Untitled_Pipeline"),
        "parameters": parameters,
        "processors": proc_specs,
        "connections": conn_specs,
        "used_rels": dict(used_rels),
    }

def calculate_auto_layout(processors: list[dict], connections: list[dict]) -> dict[str, tuple[int, int]]:
    in_degree = {p["name"]: 0 for p in processors}
    adjacency: dict[str, list[str]] = defaultdict(list)
    for c in connections:
        adjacency[c["source"]].append(c["destination"])
        in_degree[c["destination"]] = in_degree.get(c["destination"], 0) + 1

    queue = deque([name for name, deg in in_degree.items() if deg == 0])
    levels: dict[str, int] = {}
    current_level = 0
    while queue:
        for _ in range(len(queue)):
            node = queue.popleft()
            levels.setdefault(node, current_level)
            for nxt in adjacency[node]:
                in_degree[nxt] -= 1
                if in_degree[nxt] == 0:
                    queue.append(nxt)
        current_level += 1

    positions: dict[str, tuple[int, int]] = {}
    level_counts: dict[int, int] = defaultdict(int)
    for p in processors:
        name = p["name"]
        lvl = levels.get(name, 0)
        row = level_counts[lvl]
        level_counts[lvl] += 1
        positions[name] = (200 + lvl * 380, 150 + row * 220)
    return positions

# =====================================================================
# 階段 2: 核心 NiFi I/O 操作 (無狀態, 單元可測)
# =====================================================================
def ensure_process_group(target_name: str):
    root_id = nipyapi.canvas.get_root_pg_id()
    root_pg = nipyapi.canvas.get_process_group(root_id, identifier_type="id")

    existing = [
        pg for pg in nipyapi.canvas.list_all_process_groups(root_id)
        if pg.component.name == target_name
    ]
    if existing:
        return existing[0]
    return nipyapi.canvas.create_process_group(root_pg, target_name, (300, 200))

def sync_parameter_context_and_bind(target_pg, context_name: str, parameters: dict[str, str]):
    if not parameters:
        return None

    all_contexts = nipyapi.parameters.list_all_parameter_contexts()
    existing_ctx = [ctx for ctx in all_contexts if ctx.component.name == context_name]

    param_dto_list = [
        nipyapi.nifi.ParameterEntity(
            parameter=nipyapi.nifi.ParameterDTO(
                name=k,
                value=str(v),
                sensitive=False
            )
        )
        for k, v in parameters.items()
    ]

    if existing_ctx:
        ctx_entity = existing_ctx[0]
        ctx_entity = nipyapi.parameters.get_parameter_context(ctx_entity.id, identifier_type="id")
        
        # 關鍵修正：在實體頂層與 component 內同時填入 id，避免 400 錯誤
        update_dto = nipyapi.nifi.ParameterContextDTO(
            id=ctx_entity.id,
            name=context_name,
            parameters=param_dto_list
        )
        req_update_entity = nipyapi.nifi.ParameterContextEntity(
            id=ctx_entity.id,
            revision=ctx_entity.revision,
            component=update_dto
        )
        ctx_entity = nipyapi.nifi.ParameterContextsApi().update_parameter_context(
            ctx_entity.id,
            req_update_entity
        )
        print(f"🔄 更新 Parameter Context: [{context_name}]")
    else:
        req_entity = nipyapi.nifi.ParameterContextEntity(
            revision=nipyapi.nifi.RevisionDTO(version=0),
            component=nipyapi.nifi.ParameterContextDTO(
                name=context_name,
                parameters=param_dto_list
            )
        )
        ctx_entity = nipyapi.nifi.ParameterContextsApi().create_parameter_context(req_entity)
        print(f"✅ 成功建立 Parameter Context: [{context_name}] (ID: {ctx_entity.id})")

    # 綁定至目標 Process Group
    target_pg = nipyapi.canvas.get_process_group(target_pg.id, identifier_type="id")
    current_bound = target_pg.component.parameter_context

    if not current_bound or current_bound.id != ctx_entity.id:
        update_pg_entity = nipyapi.nifi.ProcessGroupEntity(
            id=target_pg.id,
            revision=target_pg.revision,
            component=nipyapi.nifi.ProcessGroupDTO(
                id=target_pg.id,
                parameter_context=nipyapi.nifi.ParameterContextReferenceEntity(
                    id=ctx_entity.id,
                    permissions=nipyapi.nifi.PermissionsDTO(can_read=True, can_write=True),
                    component=nipyapi.nifi.ParameterContextReferenceDTO(
                        id=ctx_entity.id,
                        name=context_name
                    )
                )
            )
        )
        nipyapi.nifi.ProcessGroupsApi().update_process_group(target_pg.id, update_pg_entity)
        print(f"🔗 Process Group [{target_pg.component.name}] 已成功綁定 Context [{context_name}]！")

    return ctx_entity

def interpolate_value(val: Any, parameters: dict[str, str]) -> Any:
    """替換字串中的 #{VAR} 變數"""
    if isinstance(val, str):
        for k, v in parameters.items():
            val = val.replace(f"#{{{k}}}", str(v))
    return val

def sync_single_processor(
    target_pg,
    proc_spec: dict,
    position: tuple[int, int],
    used_relationships: set[str],
    existing_processors: dict[str, Any],
    parameters: dict[str, str],
) -> tuple[str, Any]:
    name = proc_spec["name"]
    if name in existing_processors:
        proc_entity = existing_processors[name]
        if proc_entity.component.state == "RUNNING":
            nipyapi.canvas.schedule_processor(proc_entity, scheduled=False, refresh=True)
            proc_entity = nipyapi.canvas.get_processor(proc_entity.id, identifier_type="id")
    else:
        proc_type = nipyapi.canvas.get_processor_type(proc_spec["type"])
        proc_entity = nipyapi.canvas.create_processor(target_pg, proc_type, position, name=name)

    all_relationships = {r.name for r in proc_entity.component.relationships}
    unused = list(all_relationships - used_relationships)

    sched = proc_spec.get("scheduling", {})
    raw_period = sched.get("period", "0 sec")
    resolved_period = interpolate_value(raw_period, parameters)

    config = nipyapi.nifi.ProcessorConfigDTO(
        properties=proc_spec.get("properties", {}),
        scheduling_strategy=sched.get("strategy", "TIMER_DRIVEN"),
        scheduling_period=resolved_period,
        concurrently_schedulable_task_count=sched.get("concurrent_tasks", 1),
        execution_node=sched.get("execution_node", "ALL"),
        auto_terminated_relationships=unused,
    )
    return name, nipyapi.canvas.update_processor(proc_entity, config)

def sync_single_connection(
    conn_spec: dict,
    active_processors: dict[str, Any],
    existing_connections: list[Any],
) -> None:
    source = active_processors.get(conn_spec["source"])
    destination = active_processors.get(conn_spec["destination"])
    if source is None or destination is None:
        return

    already_connected = any(
        c.component.source.id == source.id and c.component.destination.id == destination.id
        for c in existing_connections
    )
    if not already_connected:
        conn = nipyapi.canvas.create_connection(
            source, destination, relationships=conn_spec.get("relationships", [])
        )
        fc = conn_spec.get("flow_control", {})
        if fc:
            conn_config = nipyapi.nifi.ConnectionDTO(
                back_pressure_object_threshold=fc.get("back_pressure_count", 10000),
                back_pressure_data_size_threshold=fc.get("back_pressure_size", "1 GB"),
                load_balance_strategy=fc.get("load_balance_strategy", "DO_NOT_LOAD_BALANCE")
            )
            nipyapi.canvas.update_connection(conn, conn_config)

# =====================================================================
# 階段 3: 線性編排器 (含度量與 Context 綁定)
# =====================================================================
def run_deployment_pipeline(spec: dict, max_workers: int = 5, auto_start: bool = False) -> dict:
    metrics = {}
    t_start = time.perf_counter()

    # 1. 規格解析
    t0 = time.perf_counter()
    parsed = analyze_spec(spec)
    metrics["stage1_parse_ms"] = (time.perf_counter() - t0) * 1000

    # 2. Fork 1: 座標 (CPU) 與 PG 查找 (I/O)
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_pg = executor.submit(ensure_process_group, parsed["pipeline_name"])
        future_layout = executor.submit(
            calculate_auto_layout, parsed["processors"], parsed["connections"]
        )
        target_pg = future_pg.result()
        positions = future_layout.result()
    metrics["stage2_fork1_pg_and_layout_ms"] = (time.perf_counter() - t0) * 1000

    # 2.5 建立 Parameter Context 並綁定至 Process Group
    t0 = time.perf_counter()
    ctx_name = f"{parsed['pipeline_name']}_Context"
    sync_parameter_context_and_bind(target_pg, ctx_name, parsed["parameters"])
    metrics["stage2_5_parameter_context_ms"] = (time.perf_counter() - t0) * 1000

    # 3. Fork 2: 節點建立與合併配置
    t0 = time.perf_counter()
    existing_processors = {
        p.component.name: p for p in nipyapi.canvas.list_all_processors(target_pg.id)
    }
    active_processors: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                sync_single_processor,
                target_pg,
                p,
                positions.get(p["name"], (200, 200)),
                set(parsed["used_rels"].get(p["name"], set())),
                existing_processors,
                parsed["parameters"],
            )
            for p in parsed["processors"]
        ]
        for future in as_completed(futures):
            name, entity = future.result()
            active_processors[name] = entity
    metrics["stage3_fork2_processors_ms"] = (time.perf_counter() - t0) * 1000

    # 4. Fork 3: 連線建立
    t0 = time.perf_counter()
    existing_connections = nipyapi.canvas.list_all_connections(target_pg.id)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(sync_single_connection, c, active_processors, existing_connections)
            for c in parsed["connections"]
        ]
        for future in as_completed(futures):
            future.result()
    metrics["stage4_fork3_connections_ms"] = (time.perf_counter() - t0) * 1000

    # 5. 生命週期啟動
    if auto_start:
        nipyapi.canvas.schedule_process_group(target_pg.id, scheduled=True)

    metrics["total_elapsed_ms"] = (time.perf_counter() - t_start) * 1000
    return {
        "status": "SUCCESS",
        "pg_id": target_pg.id,
        "processors_count": len(active_processors),
        "connections_count": len(parsed["connections"]),
        "metrics": metrics,
    }

if __name__ == "__main__":
    with open("pipeline_spec.yaml", "r", encoding="utf-8") as f:
        spec = yaml.safe_load(f)
    res = run_deployment_pipeline(spec, max_workers=4, auto_start=True)
    print(res)