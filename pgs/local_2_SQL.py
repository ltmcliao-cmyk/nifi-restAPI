#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - Local_2_SQL 完整流程圖模組 (徹底解決 Controller Service 400 錯誤)
"""
import os
import sys
import time
import json
import traceback
import nipyapi

# 強制無緩衝輸出，確保 CI/CD 即時顯示日誌
sys.stdout.reconfigure(line_buffering=True)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

PROCESS_GROUP_ID = "10d7800a-01a1-1000-e2d4-9b50ad6cf816"
PROCESS_GROUP_NAME = "Local_2_SQL"

# Processor 名稱定義
LIST_PROC_NAME = "List Local Raw Files"
FETCH_PROC_NAME = "Fetch Raw Content (Read Only)"
ROUTE_PROC_NAME = "Route Table Type"
PREPEND_PROC_NAME = "Wrap SQL Header (Prepend)"
APPEND_PROC_NAME = "Wrap SQL Footer (Append)"
PUT_SQL_PROC_NAME = "PutSQL to PostgreSQL RAW"

DEFAULT_DB_CONFIG = {
    "url": "jdbc:postgresql://postgres:5432/pipeline_db",
    "driver_class": "org.postgresql.Driver",
    "driver_location": "/opt/nifi/nifi-current/drivers/postgresql-42.7.3.jar",
    "user": "postgres",
    "password": "postgrespassword123",
}

DEFAULT_RAW_DIR = "/opt/nifi/nifi-current/data/raw"


def log(msg, level="INFO"):
    """統一的 CI/CD 格式化排查日誌輸出"""
    prefix = {
        "INFO": "[INFO]",
        "WARN": "[⚠️ WARN]",
        "ERR": "[❌ ERROR]",
        "OK": "[✅ SUCCESS]",
        "STEP": "[🚀 STEP]",
        "DIAG": "[🔍 DIAG]"
    }.get(level, f"[{level}]")
    print(f"[DEPLOY-LOG {time.strftime('%H:%M:%S')}] {prefix} {msg}", flush=True)


def parse_api_exception(e):
    """解析 NiFi API 例外資訊"""
    if hasattr(e, "status") or hasattr(e, "body"):
        body = getattr(e, "body", "")
        try:
            body = json.loads(body)
        except Exception:
            pass
        return f"Status: {getattr(e, 'status', 'N/A')}, Reason: {getattr(e, 'reason', 'N/A')}, Body: {body}"
    return str(e)


def get_exact_processor_type(type_name):
    """精準提取單一 DocumentedTypeDTO 實例"""
    entity = nipyapi.nifi.FlowApi().get_processor_types()
    types_list = getattr(entity, "processor_types", [])
    for t in types_list:
        if t.type == type_name or t.type.endswith(f".{type_name}"):
            return t
    res = nipyapi.canvas.get_processor_type(type_name)
    if isinstance(res, list) and res:
        return res[0]
    if isinstance(res, nipyapi.nifi.DocumentedTypeDTO):
        return res
    raise ValueError(f"無法在 NiFi 註冊表中定位處理器型別: {type_name}")


def stop_processor_safely(proc):
    """安全停止指定處理器並驗證至 STOPPED 狀態"""
    if not proc:
        return
    proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]
    if proc.component.state == "RUNNING":
        log(f"發送停止命令至處理器: [{proc.component.name}]...", "STEP")
        try:
            nipyapi.canvas.schedule_processor(proc, scheduled=False)
        except Exception as e:
            log(f"停止提示: {parse_api_exception(e)}", "WARN")
        for _ in range(15):
            time.sleep(0.5)
            proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
            if isinstance(proc, list):
                proc = proc[0]
            if proc.component.state == "STOPPED":
                log(f"處理器 [{proc.component.name}] 已安全停止！", "OK")
                return


def start_processor_safely(proc):
    """安全啟動指定處理器"""
    if not proc:
        return
    proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]
    if proc.component.state == "STOPPED":
        log(f"發送啟動命令至: [{proc.component.name}]...", "STEP")
        try:
            nipyapi.canvas.schedule_processor(proc, scheduled=True)
            log(f"處理器 [{proc.component.name}] 已成功啟動！", "OK")
        except Exception as e:
            log(f"啟動提示: {parse_api_exception(e)}", "WARN")


def safe_purge_and_delete_connection(conn):
    """先清空佇列殘留數據，再安全刪除連線，避開 409 Conflict"""
    if not conn:
        return
    try:
        log(f"清空連線佇列 (ID: {conn.id})...", "INFO")
        try:
            nipyapi.canvas.purge_connection(conn.id)
        except Exception:
            pass
        time.sleep(0.5)
        nipyapi.canvas.delete_connection(conn)
        log(f"連線已安全刪除 (ID: {conn.id})", "OK")
    except Exception as e:
        log(f"連線刪除提示: {parse_api_exception(e)}", "WARN")


def configure_processor_clean(proc, desired_properties, auto_terminated_rels=None):
    """屬性精確對齊與舊屬性清理"""
    descriptors = proc.component.config.descriptors or {}
    current_props = proc.component.config.properties or {}
    clean_props = {k: None for k in current_props.keys()}

    for target_name, value in desired_properties.items():
        matched_key = None
        for k, desc in descriptors.items():
            disp_name = getattr(desc, "display_name", "") or ""
            formal_name = getattr(desc, "name", "") or ""
            if target_name.lower() in [k.lower(), disp_name.lower(), formal_name.lower()]:
                matched_key = k
                break
        if matched_key:
            clean_props[matched_key] = value
        else:
            log(f"[{proc.component.name}] 警告: 屬性 '{target_name}' 不存在於官方規格！", "WARN")

    proc.component.config.properties = clean_props
    if auto_terminated_rels:
        valid_rels = [r.name for r in proc.component.relationships]
        proc.component.config.auto_terminated_relationships = [
            r for r in auto_terminated_rels if r in valid_rels
        ]
    return nipyapi.canvas.update_processor(proc, proc.component.config)


def ensure_local_controller_service(local_pg, service_type, service_name, properties=None):
    """
    建立或確認 Controller Service 狀態。
    若需修改屬性，確保無痛處理 DISABLE -> UPDATE -> ENABLE 流程，防範 400 錯誤。
    """
    log(f"檢查 Controller Service: '{service_name}'", "STEP")
    services = nipyapi.canvas.list_all_controllers(local_pg.id)
    svc = next((s for s in services if s.component.name == service_name), None)

    if not svc:
        log(f"在 PG [{local_pg.id}] 中建立服務: {service_name}...", "INFO")
        svc = nipyapi.canvas.create_controller(
            parent_pg=local_pg,
            controller_type=service_type,
            name=service_name
        )
        log(f"服務建立成功，UUID: {svc.id}", "OK")
    else:
        log(f"找到現有服務 UUID: {svc.id}，狀態: {svc.component.state}", "DIAG")

    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    # 檢查屬性是否完全一致，若不同才進行更新
    if properties:
        current_props = svc.component.properties or {}
        needs_update = False
        for k, v in properties.items():
            if current_props.get(k) != v:
                needs_update = True
                break

        if needs_update:
            log(f"Controller Service 屬性變更，執行安全停用與更新...", "STEP")
            # 1. 若目前為 ENABLED，必須先停用 (Disable)
            if svc.component.state != "DISABLED":
                try:
                    nipyapi.canvas.schedule_controller(svc, scheduled=False)
                except Exception as e:
                    log(f"停用發送提示: {parse_api_exception(e)}", "WARN")

                for _ in range(15):
                    time.sleep(0.5)
                    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
                    if isinstance(svc, list):
                        svc = svc[0]
                    if svc.component.state == "DISABLED":
                        log(f"Controller Service 已成功轉為 DISABLED", "OK")
                        break

            # 2. 更新 Controller Service 配置
            try:
                svc.component.properties = properties
                nipyapi.canvas.update_controller(svc, svc.component)
                log(f"Controller Service 配置更新成功！", "OK")
            except Exception as e:
                log(f"更新配置提示: {parse_api_exception(e)}", "WARN")

    # 3. 若服務為 DISABLED，發送啟用命令 (Enable)
    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    if svc.component.state != "ENABLED":
        log(f"發送啟用命令至 {service_name}...", "STEP")
        try:
            nipyapi.canvas.schedule_controller(svc, scheduled=True)
        except Exception as e:
            log(f"啟用調度提示: {parse_api_exception(e)}", "WARN")

        for _ in range(20):
            time.sleep(0.5)
            svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
            if isinstance(svc, list):
                svc = svc[0]
            if svc.component.state == "ENABLED":
                log(f"Controller Service '{service_name}' 已成功轉為 ENABLED！", "OK")
                break

    return svc


def setup_local_2_sql_pipeline(local_pg, dbcp_svc, input_dir=DEFAULT_RAW_DIR, table_name="raw_bike_availability"):
    """
    建立完整的 6-Processor Pipeline
    """
    log("開始部署 Local_2_SQL 完整 Pipeline...", "STEP")

    procs = nipyapi.canvas.list_all_processors(local_pg.id)
    proc_map = {p.component.name: p for p in procs}

    # 1. 清空舊連線
    conns = nipyapi.canvas.list_all_connections(local_pg.id)
    for c in conns:
        safe_purge_and_delete_connection(c)

    base_x, base_y = 100, 100

    # -------------------------------------------------------------
    # 1. List Local Raw Files (ListFile)
    # -------------------------------------------------------------
    list_proc = proc_map.get(LIST_PROC_NAME)
    if not list_proc:
        log("建立 ListFile 處理器...", "STEP")
        list_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=get_exact_processor_type("ListFile"),
            location=(base_x, base_y),
            name=LIST_PROC_NAME
        )
    list_props = {
        "Input Directory": input_dir,
        "Listing Strategy": "Tracking Timestamps",
        "Recurse Subdirectories": "true",
        "Path Filter": ".*",
        "Minimum File Age": "1 min"
    }
    list_proc = configure_processor_clean(list_proc, list_props)
    nipyapi.canvas.update_processor(list_proc, {
        "config": {
            "schedulingPeriod": "1 min",
            "concurrentlySchedulableTaskCount": 1
        }
    })
    log(f"ListFile 配置完成 (UUID: {list_proc.id})", "OK")

    # -------------------------------------------------------------
    # 2. Fetch Raw Content (FetchFile)
    # -------------------------------------------------------------
    fetch_proc = proc_map.get(FETCH_PROC_NAME)
    if not fetch_proc:
        log("建立 FetchFile 處理器...", "STEP")
        fetch_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=get_exact_processor_type("FetchFile"),
            location=(base_x + 300, base_y),
            name=FETCH_PROC_NAME
        )
    fetch_props = {
        "File to Fetch": "${absolute.path}/${filename}",
        "Completion Strategy": "None"
    }
    fetch_proc = configure_processor_clean(
        fetch_proc,
        fetch_props,
        auto_terminated_rels=["failure", "not.found", "permission.denied"]
    )
    log(f"FetchFile 配置完成 (UUID: {fetch_proc.id})", "OK")

    # -------------------------------------------------------------
    # 3. Route Table Type (RouteOnAttribute)
    # -------------------------------------------------------------
    route_proc = proc_map.get(ROUTE_PROC_NAME)
    if not route_proc:
        log("建立 RouteOnAttribute 處理器...", "STEP")
        route_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=get_exact_processor_type("RouteOnAttribute"),
            location=(base_x + 600, base_y),
            name=ROUTE_PROC_NAME
        )
    route_props = {
        "Routing Strategy": "Route to Property name",
        "matched": "${filename:endsWith('.json')}"
    }
    route_proc = configure_processor_clean(
        route_proc,
        route_props,
        auto_terminated_rels=["unmatched"]
    )
    log(f"RouteOnAttribute 配置完成 (UUID: {route_proc.id})", "OK")

    # -------------------------------------------------------------
    # 4. Wrap SQL Header (Prepend - ReplaceText)
    # -------------------------------------------------------------
    prepend_proc = proc_map.get(PREPEND_PROC_NAME)
    if not prepend_proc:
        log("建立 Prepend 處理器...", "STEP")
        prepend_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=get_exact_processor_type("ReplaceText"),
            location=(base_x + 900, base_y),
            name=PREPEND_PROC_NAME
        )
    prepend_props = {
        "Replacement Strategy": "Prepend",
        "Replacement Value": f"INSERT INTO {table_name} (source_endpoint, payload) VALUES ('Bike-Availability-Taipei', $raw_json$",
        "Evaluation Mode": "Entire text"
    }
    prepend_proc = configure_processor_clean(
        prepend_proc,
        prepend_props,
        auto_terminated_rels=["failure"]
    )
    log(f"Prepend 配置完成 (UUID: {prepend_proc.id})", "OK")

    # -------------------------------------------------------------
    # 5. Wrap SQL Footer (Append - ReplaceText)
    # -------------------------------------------------------------
    append_proc = proc_map.get(APPEND_PROC_NAME)
    if not append_proc:
        log("建立 Append 處理器...", "STEP")
        append_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=get_exact_processor_type("ReplaceText"),
            location=(base_x + 1200, base_y),
            name=APPEND_PROC_NAME
        )
    append_props = {
        "Replacement Strategy": "Append",
        "Replacement Value": "$raw_json$::jsonb);",
        "Evaluation Mode": "Entire text"
    }
    append_proc = configure_processor_clean(
        append_proc,
        append_props,
        auto_terminated_rels=["failure"]
    )
    log(f"Append 配置完成 (UUID: {append_proc.id})", "OK")

    # -------------------------------------------------------------
    # 6. PutSQL to PostgreSQL RAW (PutSQL)
    # -------------------------------------------------------------
    putsql_proc = proc_map.get(PUT_SQL_PROC_NAME)
    if not putsql_proc:
        log("建立 PutSQL 處理器...", "STEP")
        putsql_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=get_exact_processor_type("PutSQL"),
            location=(base_x + 1500, base_y),
            name=PUT_SQL_PROC_NAME
        )
    putsql_props = {
        "JDBC Connection Pool": dbcp_svc.id,
        "Support Fragmented Transactions": "false",
        "Transaction Timeout": "30 sec",
        "Batch Size": "100"
    }
    putsql_proc = configure_processor_clean(
        putsql_proc,
        putsql_props,
        auto_terminated_rels=["success", "failure", "retry"]
    )
    log(f"PutSQL 配置完成 (UUID: {putsql_proc.id})", "OK")

    # 建立 5 條管道連線
    log("建置完整 Pipeline 流向連線...", "STEP")

    nipyapi.canvas.create_connection(
        source=list_proc,
        target=fetch_proc,
        relationships=["success"],
        name="Listed to Fetch"
    )

    nipyapi.canvas.create_connection(
        source=fetch_proc,
        target=route_proc,
        relationships=["success"],
        name="Fetched to Route"
    )

    nipyapi.canvas.create_connection(
        source=route_proc,
        target=prepend_proc,
        relationships=["matched"],
        name="Matched to Prepend"
    )

    nipyapi.canvas.create_connection(
        source=prepend_proc,
        target=append_proc,
        relationships=["success"],
        name="Prepended to Append"
    )

    nipyapi.canvas.create_connection(
        source=append_proc,
        target=putsql_proc,
        relationships=["success"],
        name="SQL Complete to DB"
    )

    log("連線建立完成！開始啟動所有 Processor...", "STEP")

    all_pipeline_procs = [putsql_proc, append_proc, prepend_proc, route_proc, fetch_proc, list_proc]
    for target in all_pipeline_procs:
        log(f"等待處理器 [{target.component.name}] 校驗...", "INFO")
        for attempt in range(1, 21):
            time.sleep(0.5)
            p_fresh = nipyapi.canvas.get_processor(target.id, identifier_type="id")
            if isinstance(p_fresh, list):
                p_fresh = p_fresh[0]
            errors = p_fresh.component.validation_errors or []
            if not errors:
                log(f"處理器 [{p_fresh.component.name}] 校驗通過！", "OK")
                break
            if attempt == 20:
                raise RuntimeError(f"{p_fresh.component.name} 驗證失敗: {errors}")
        start_processor_safely(target)


def dump_process_group_health(local_pg):
    """診斷報告輸出"""
    log("==========================================", "DIAG")
    log(f"=== [HEALTH-CHECK] PG: {local_pg.component.name} 診斷 ===", "DIAG")
    log("==========================================", "DIAG")
    procs = nipyapi.canvas.list_all_processors(local_pg.id)
    log(f"--- Processors (共 {len(procs)} 個) ---", "DIAG")
    for p in procs:
        state_badge = f"[{p.component.state}]"
        log(f"  * {state_badge:<10} {p.component.name} (UUID: {p.id})", "DIAG")
    conns = nipyapi.canvas.list_all_connections(local_pg.id)
    log(f"--- Connections (共 {len(conns)} 條) ---", "DIAG")
    for c in conns:
        src = c.component.source.name
        dst = c.component.destination.name
        log(f"  * 連線: '{c.component.name}' [{src} -> {dst}]", "DIAG")
    log("==========================================", "DIAG")


def create_local_2_sql_pg(parent_pg=None, *args, **kwargs):
    """主進入點"""
    log("==========================================", "STEP")
    log("=== [START] Local_2_SQL 完整部署 ===", "STEP")
    log("==========================================", "STEP")
    try:
        local_pg = nipyapi.canvas.get_process_group(PROCESS_GROUP_ID, identifier_type="id")
        if isinstance(local_pg, list):
            local_pg = local_pg[0]
        if not local_pg:
            local_pg = nipyapi.canvas.get_process_group(PROCESS_GROUP_NAME, identifier_type="name")
            if isinstance(local_pg, list) and local_pg:
                local_pg = local_pg[0]
        if not local_pg:
            raise ValueError(f"找不到 Process Group: {PROCESS_GROUP_NAME}")

        log(f"鎖定 Process Group: {local_pg.component.name} (UUID: {local_pg.id})", "OK")

        # 1. 停止所有現有 Processors，避免占用 Controller Service 導致無法修改
        procs = nipyapi.canvas.list_all_processors(local_pg.id)
        log("首先停止 PG 內所有 Processor，釋放 Controller Service 鎖定...", "STEP")
        for p in procs:
            stop_processor_safely(p)

        # 2. 安全配置/更新 DBCP Controller Service
        db_config = kwargs.get("db_config", DEFAULT_DB_CONFIG)
        dbcp_properties = {
            "Database Connection URL": db_config["url"],
            "Database Driver Class Name": db_config["driver_class"],
            "database-driver-locations": db_config["driver_location"],
            "Database User": db_config["user"],
            "Password": db_config["password"],
        }
        dbcp_svc = ensure_local_controller_service(
            local_pg=local_pg,
            service_type="org.apache.nifi.dbcp.DBCPConnectionPool",
            service_name="PostgreSQL_DBCP_Pool",
            properties=dbcp_properties,
        )

        # 3. 部署 6 節點完整管道
        input_dir = kwargs.get("input_dir", DEFAULT_RAW_DIR)
        table_name = kwargs.get("table_name", "raw_bike_availability")
        setup_local_2_sql_pipeline(local_pg, dbcp_svc, input_dir=input_dir, table_name=table_name)

        # 4. 輸出健康檢查
        dump_process_group_health(local_pg)

        log("==========================================", "OK")
        log("=== [FINISH] Local_2_SQL 部署順利完成 ===", "OK")
        log("==========================================", "OK")
        return local_pg

    except Exception as e:
        log(f"部署過程發生致命例外: {parse_api_exception(e)}", "ERR")
        traceback.print_exc()
        raise e


if __name__ == "__main__":
    nipyapi.config.nifi_config.host = "http://127.0.0.1:8080/nifi-api"
    pg = create_local_2_sql_pg()
    try:
        nipyapi.canvas.schedule_process_group(pg.id, scheduled=True)
    except Exception as e:
        log(f"schedule_process_group 提示: {parse_api_exception(e)}", "WARN")