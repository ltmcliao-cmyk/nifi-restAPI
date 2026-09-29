"""
deploy.py —— 支援 Parameter Context、Controller Services、度量遙測與 Fork/Join 並行的純函數式 NiFi 部署管線
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


def resolve_processor_type(type_identifier: str):
    """精確解析 NiFi Processor 類型，回傳 DocumentedTypeDTO"""
    proc_type = nipyapi.canvas.get_processor_type(type_identifier)
    if not proc_type and "." in type_identifier:
        short_name = type_identifier.split(".")[-1]
        proc_type = nipyapi.canvas.get_processor_type(short_name)

    if not proc_type:
        all_types = nipyapi.canvas.list_all_processor_types()
        for t in all_types:
            if t.type == type_identifier:
                proc_type = t
                break

    if isinstance(proc_type, list):
        proc_type = proc_type[0] if proc_type else None

    if not proc_type or not isinstance(proc_type, nipyapi.nifi.DocumentedTypeDTO):
        raise ValueError(
            f"無法在目標 NiFi 找到 Processor 類型: '{type_identifier}'。"
            f"請檢查類別名稱或 NAR 是否已正確掛載。"
        )
    return proc_type


def resolve_controller_service_type(type_identifier: str):
    """精確解析 NiFi Controller Service 類型，回傳 DocumentedTypeDTO"""
    all_types = nipyapi.canvas.list_all_controller_types()

    # 1. 優先以 FQCN 完整類別路徑比對
    for t in all_types:
        if t.type == type_identifier:
            return t

    # 2. 次之以類別短名比對 (例如 DBCPConnectionPool)
    short_name = type_identifier.split(".")[-1]
    for t in all_types:
        if t.type.endswith(f".{short_name}") or t.type == short_name:
            return t

    raise ValueError(
        f"無法在目標 NiFi 找到 Controller Service 類型: '{type_identifier}'。"
        f"請確認該服務類型是否支援。"
    )


# =====================================================================
# 階段 1: 規格解析與圖論排版 (純函數, CPU-bound)
# =====================================================================
def analyze_spec(spec: dict) -> dict:
    proc_specs = spec.get("processors", [])
    conn_specs = spec.get("connections", [])
    cs_specs = spec.get("controller_services", [])
    parameters = spec.get("parameters", {})

    used_rels: dict[str, set[str]] = defaultdict(set)
    for c in conn_specs:
        src = c.get("source") or c.get("from")
        rels = c.get("relationships") or ([c["relationship"]] if "relationship" in c else [])
        for r in rels:
            if src:
                used_rels[src].add(r)

    return {
        "pipeline_name": spec.get("pipeline_name", "Untitled_Pipeline"),
        "parameters": parameters,
        "controller_services": cs_specs,
        "processors": proc_specs,
        "connections": conn_specs,
        "used_rels": dict(used_rels),
    }


def calculate_auto_layout(processors: list[dict], connections: list[dict]) -> dict[str, tuple[int, int]]:
    in_degree = {p["name"]: 0 for p in processors}
    adjacency: dict[str, list[str]] = defaultdict(list)
    for c in connections:
        src = c.get("source") or c.get("from")
        dst = c.get("destination") or c.get("to")
        if not src or not dst:
            raise KeyError(f"連線配置缺少 source 或 destination: {c}")
        adjacency[src].append(dst)
        in_degree[dst] = in_degree.get(dst, 0) + 1

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
        ctx_entity.component.parameters = param_dto_list
        ctx_entity = nipyapi.nifi.ParameterContextsApi().update_parameter_context(
            id=ctx_entity.id,
            body=ctx_entity
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

    target_pg = nipyapi.canvas.get_process_group(target_pg.id, identifier_type="id")
    current_bound = target_pg.component.parameter_context

    if not current_bound or current_bound.id != ctx_entity.id:
        target_pg.component.parameter_context = nipyapi.nifi.ParameterContextReferenceEntity(
            id=ctx_entity.id,
            component=nipyapi.nifi.ParameterContextReferenceDTO(
                id=ctx_entity.id,
                name=context_name
            )
        )
        nipyapi.nifi.ProcessGroupsApi().update_process_group(
            id=target_pg.id,
            body=target_pg
        )
        print(f"🔗 Process Group [{target_pg.component.name}] 已成功綁定 Context [{context_name}]！")

    return ctx_entity


def sync_controller_services(target_pg, cs_specs: list[dict]) -> dict[str, str]:
    """建立、更新配置並啟用 Controller Services，回傳 {Service_Name: Service_UUID} 映射字典"""
    if not cs_specs:
        return {}

    flow_api = nipyapi.nifi.FlowApi()
    cs_api = nipyapi.nifi.ControllerServicesApi()
    pg_api = nipyapi.nifi.ProcessGroupsApi()

    # 1. 取得目標 Process Group 內的現有 Controller Services
    existing_resp = flow_api.get_controller_services_from_group(id=target_pg.id)
    existing_cs = {
        cs.component.name: cs for cs in (existing_resp.controller_services or [])
    }

    cs_id_map: dict[str, str] = {}
    ordered_names: list[str] = []

    # 2. 建立或取得所有 Controller Services 實體與 UUID
    for cs_spec in cs_specs:
        name = cs_spec["name"]
        ordered_names.append(name)
        if name in existing_cs:
            cs_entity = existing_cs[name]
        else:
            cs_type = resolve_controller_service_type(cs_spec["type"])
            req_body = nipyapi.nifi.ControllerServiceEntity(
                revision=nipyapi.nifi.RevisionDTO(version=0),
                component=nipyapi.nifi.ControllerServiceDTO(
                    name=name,
                    type=cs_type.type,
                    bundle=cs_type.bundle
                )
            )
            if hasattr(pg_api, "create_controller_service1"):
                cs_entity = pg_api.create_controller_service1(id=target_pg.id, body=req_body)
            else:
                cs_entity = pg_api.create_controller_service(id=target_pg.id, body=req_body)

        cs_id_map[name] = cs_entity.id

    # 3. 替換相互參照的 Service 名稱並更新配置
    for cs_spec in cs_specs:
        name = cs_spec["name"]
        cs_id = cs_id_map[name]
        cs_entity = cs_api.get_controller_service(id=cs_id)

        # 替換屬性中參照其他 Controller Service 的名稱為實體 UUID
        raw_props = cs_spec.get("properties", {})
        resolved_props = {
            k: cs_id_map.get(str(v), str(v))
            for k, v in raw_props.items()
        }

        # 若已啟用則先停用以允許修改屬性
        if cs_entity.component.state == "ENABLED":
            try:
                run_status_body = nipyapi.nifi.ControllerServiceRunStatusEntity(
                    revision=cs_entity.revision,
                    state="DISABLED"
                )
                cs_entity = cs_api.update_run_status(id=cs_id, body=run_status_body)
                time.sleep(0.5)
            except Exception:
                pass

        # 寫入最新配置
        cs_entity = cs_api.get_controller_service(id=cs_id)
        cs_entity.component.properties = resolved_props
        cs_entity = cs_api.update_controller_service(id=cs_id, body=cs_entity)
        print(f"⚙️ Controller Service [{name}] 配置完成 (ID: {cs_id})")

    # 4. 按順序啟用 Controller Services 並輪詢確認狀態
    for name in ordered_names:
        cs_id = cs_id_map[name]
        curr = cs_api.get_controller_service(id=cs_id)
        if curr.component.state != "ENABLED":
            try:
                run_status_body = nipyapi.nifi.ControllerServiceRunStatusEntity(
                    revision=curr.revision,
                    state="ENABLED"
                )
                cs_api.update_run_status(id=curr.id, body=run_status_body)

                # 等待直到真正變為 ENABLED (最多等待 10 秒)
                for _ in range(10):
                    time.sleep(1)
                    curr = cs_api.get_controller_service(id=curr.id)
                    if curr.component.state == "ENABLED":
                        break
            except Exception as e:
                logger.warning(f"啟用 Controller Service [{name}] 時略過: {e}")

        final_state = curr.component.state
        print(f"▶️ Controller Service [{name}] 狀態: {final_state}")

    return cs_id_map


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
    cs_id_map: dict[str, str],
) -> tuple[str, Any]:
    name = proc_spec["name"]
    if name in existing_processors:
        proc_entity = existing_processors[name]
        if proc_entity.component.state == "RUNNING":
            nipyapi.canvas.schedule_processor(proc_entity, scheduled=False, refresh=True)
            proc_entity = nipyapi.canvas.get_processor(proc_entity.id, identifier_type="id")
    else:
        proc_type = resolve_processor_type(proc_spec["type"])
        proc_entity = nipyapi.canvas.create_processor(target_pg, proc_type, position, name=name)

    all_relationships = {r.name for r in proc_entity.component.relationships}
    unused = list(all_relationships - used_relationships)

    sched = proc_spec.get("scheduling", {})
    raw_period = proc_spec.get("scheduling_period") or sched.get("period", "0 sec")
    resolved_period = interpolate_value(raw_period, parameters)

    # 將屬性中所有 Controller Service 名稱轉換為對應 UUID
    raw_props = proc_spec.get("properties", {})
    resolved_props = {}
    for k, v in raw_props.items():
        v_str = str(v)
        resolved_props[k] = cs_id_map.get(v_str, v_str)

    config = nipyapi.nifi.ProcessorConfigDTO(
        properties=resolved_props,
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
    src_name = conn_spec.get("source") or conn_spec.get("from")
    dst_name = conn_spec.get("destination") or conn_spec.get("to")
    source = active_processors.get(src_name)
    destination = active_processors.get(dst_name)
    if source is None or destination is None:
        return

    relationships = conn_spec.get("relationships")
    if not relationships and "relationship" in conn_spec:
        relationships = [conn_spec["relationship"]]
    if not relationships:
        relationships = []

    already_connected = any(
        c.component.source.id == source.id and c.component.destination.id == destination.id
        for c in existing_connections
    )
    if not already_connected:
        conn = nipyapi.canvas.create_connection(
            source, destination, relationships=relationships
        )

        fc = conn_spec.get("flow_control", {})
        bp_obj = conn_spec.get("backpressure_object_threshold") or fc.get("back_pressure_count")
        bp_data = conn_spec.get("backpressure_data_threshold") or fc.get("back_pressure_size")

        needs_update = False
        if bp_obj is not None:
            conn.component.back_pressure_object_threshold = bp_obj
            needs_update = True
        if bp_data is not None:
            conn.component.back_pressure_data_size_threshold = bp_data
            needs_update = True
        if fc.get("load_balance_strategy"):
            conn.component.load_balance_strategy = fc["load_balance_strategy"]
            needs_update = True

        if needs_update:
            nipyapi.nifi.ConnectionsApi().update_connection(
                id=conn.id,
                body=conn
            )


# =====================================================================
# 階段 3: 線性編排器 (含 Controller Service 綁定)
# =====================================================================
def run_deployment_pipeline(spec: dict, max_workers: int = 5, auto_start: bool = False) -> dict:
    metrics = {}
    t_start = time.perf_counter()

    # 1. 規格解析
    t0 = time.perf_counter()
    parsed = analyze_spec(spec)
    metrics["stage1_parse_ms"] = (time.perf_counter() - t0) * 1000

    # 2. 座標 (CPU) 與 PG 查找 (I/O)
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

    # 2.8 建立並啟用 Controller Services，取得名稱與 UUID 對應表
    t0 = time.perf_counter()
    cs_id_map = sync_controller_services(target_pg, parsed["controller_services"])
    metrics["stage2_8_controller_services_ms"] = (time.perf_counter() - t0) * 1000

    # 3. 節點建立與合併配置 (使用 cs_id_map 注入 Service UUID)
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
                cs_id_map,
            )
            for p in parsed["processors"]
        ]
        for future in as_completed(futures):
            name, entity = future.result()
            active_processors[name] = entity
    metrics["stage3_fork2_processors_ms"] = (time.perf_counter() - t0) * 1000

    # 4. 連線建立
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