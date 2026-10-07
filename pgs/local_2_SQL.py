#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - Local_2_SQL 流程圖模組 (事前防禦型 ReplaceText + PutSQL 寫入版)
"""

import os
import sys
import time
import json
import traceback
import nipyapi

# 強制無緩衝輸出，確保 GitHub Actions 即時顯示日誌
sys.stdout.reconfigure(line_buffering=True)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

PROCESS_GROUP_ID = "10d7800a-01a1-1000-e2d4-9b50ad6cf816"
PROCESS_GROUP_NAME = "Local_2_SQL"
ROUTE_PROC_ID = "10d7852f-01a1-1000-7ffb-2d30bbfc2902"
ROUTE_PROC_NAME = "Route Table Type"
PUT_DB_PROC_ID = "10d78562-01a1-1000-adec-5b289ce54b88"
PUT_DB_PROC_NAME = "PutDatabaseRecord to PostgreSQL"

REPLACE_TEXT_PROC_NAME = "Format RAW JSON to SQL"
PUT_SQL_PROC_NAME = "PutSQL to PostgreSQL RAW"

DEFAULT_DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/your_database",
    "driver_class": "org.postgresql.Driver",
    "driver_location": "/opt/nifi/drivers/postgresql-42.2.14.jar",
    "user": "postgres",
    "password": "your_password",
}


def log(msg, level="INFO"):
    """統一的 CI/CD 格式化排查日誌輸出"""
    prefix = {
        "INFO": "[INFO]",
        "WARN": "[⚠️ WARN]",
        "ERR": "[❌ ERROR]",
        "OK": "[✅ SUCCESS]",
        "STEP": "[🚀 STEP]",
        "DIAG": "[🔍 DIAG]",
        "INSPECT": "[🔬 INSPECT]"
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
    """從 FlowApi 提取標準清單並解包 processor_types，安全取得 DocumentedTypeDTO"""
    log(f"向 NiFi 查詢處理器型別: '{type_name}'...", "DIAG")
    entity = nipyapi.nifi.FlowApi().get_processor_types()
    types_list = getattr(entity, "processor_types", [])

    for t in types_list:
        if t.type == type_name or t.type.endswith(f".{type_name}"):
            log(f"型別定位成功 -> {t.type}", "OK")
            return t

    res = nipyapi.canvas.get_processor_type(type_name)
    if isinstance(res, list) and res:
        return res[0]
    if isinstance(res, nipyapi.nifi.DocumentedTypeDTO):
        return res

    raise ValueError(f"無法在 NiFi 中定位處理器型別: {type_name}")


def stop_processor_safely(proc):
    """安全停止指定處理器，並等待至 STOPPED 狀態"""
    if not proc:
        return
    proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    log(f"檢查處理器狀態: '{proc.component.name}' (UUID: {proc.id}) -> 當前: {proc.component.state}", "DIAG")
    if proc.component.state == "RUNNING":
        log(f"發送停止命令至處理器: [{proc.component.name}]...", "STEP")
        try:
            nipyapi.canvas.schedule_processor(proc, scheduled=False)
        except Exception as e:
            log(f"停止處理器提示: {parse_api_exception(e)}", "WARN")

        for _ in range(15):
            time.sleep(0.5)
            proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
            if isinstance(proc, list):
                proc = proc[0]
            if proc.component.state == "STOPPED":
                log(f"處理器 [{proc.component.name}] 已安全停止！", "OK")
                return
        log(f"處理器停止逾時 (當前狀態: {proc.component.state})", "WARN")


def start_processor_safely(proc):
    """安全啟動指定處理器"""
    if not proc:
        return
    proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    if proc.component.state == "STOPPED":
        log(f"發送啟動命令至處理器: [{proc.component.name}]...", "STEP")
        try:
            nipyapi.canvas.schedule_processor(proc, scheduled=True)
            log(f"處理器 [{proc.component.name}] 已成功啟動！", "OK")
        except Exception as e:
            log(f"啟動處理器提示: {parse_api_exception(e)}", "WARN")


def apply_safe_processor_config(proc, desired_properties, auto_terminated_rels=None):
    """
    動態屬性白名單校準函式：
    1. 僅保留 descriptors 內部合法宣告的鍵名。
    2. 自動清除所有未支援的殘留歷史鍵（例如 'Search Value'、'SQL Statement'）。
    """
    descriptors = proc.component.config.descriptors or {}
    clean_props = {}

    for target_key, value in desired_properties.items():
        matched_key = None
        for k, desc in descriptors.items():
            disp_name = getattr(desc, "display_name", "") or ""
            formal_name = getattr(desc, "name", "") or ""
            if target_key.lower() in [k.lower(), disp_name.lower(), formal_name.lower()]:
                matched_key = k
                break
        
        if matched_key:
            clean_props[matched_key] = value
        else:
            log(f"[{proc.component.name}] 略過未支援的屬性: '{target_key}'", "WARN")

    proc.component.config.properties = clean_props

    if auto_terminated_rels:
        valid_rels = [r.name for r in proc.component.relationships]
        proc.component.config.auto_terminated_relationships = [
            r for r in auto_terminated_rels if r in valid_rels
        ]

    return nipyapi.canvas.update_processor(proc, proc.component.config)


def ensure_local_controller_service(local_pg, service_type, service_name, properties=None):
    """建立或更新 Controller Service，具備冪等性避免 409 Conflict"""
    log(f"檢查 Controller Service: '{service_name}'", "STEP")
    services = nipyapi.canvas.list_all_controllers(local_pg.id)
    svc = next((s for s in services if s.component.name == service_name), None)

    if not svc:
        log(f"服務不存在，在 PG [{local_pg.id}] 中建立: {service_name} ({service_type})", "INFO")
        svc = nipyapi.canvas.create_controller(
            parent_pg=local_pg,
            controller_type=service_type,
            name=service_name
        )
        log(f"服務建立成功，UUID: {svc.id}", "OK")
    else:
        log(f"找到現有服務 UUID: {svc.id}，當前狀態: {svc.component.state}", "DIAG")

    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    needs_update = False
    if properties:
        current_props = svc.component.properties or {}
        diffs = {k: (current_props.get(k), v) for k, v in properties.items() if current_props.get(k) != v}
        if diffs:
            needs_update = True
            log(f"服務屬性需要更新: {diffs}", "DIAG")

    if needs_update:
        if svc.component.state != "DISABLED":
            log(f"暫停 {service_name} 以便更新屬性...", "INFO")
            try:
                nipyapi.canvas.schedule_controller(svc, scheduled=False)
            except Exception as e:
                log(f"停用服務提示: {parse_api_exception(e)}", "WARN")

            for _ in range(15):
                time.sleep(0.5)
                svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
                if isinstance(svc, list):
                    svc = svc[0]
                if svc.component.state == "DISABLED":
                    break

        if svc.component.state == "DISABLED":
            log(f"正在更新 {service_name} 屬性配置...", "STEP")
            current_props = svc.component.properties or {}
            current_props.update(properties)
            svc.component.properties = current_props
            svc = nipyapi.canvas.update_controller(svc, svc.component)
            log(f"屬性更新成功！", "OK")

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
                log(f"Controller Service '{service_name}' 成功轉為 ENABLED！", "OK")
                break

    return svc


def setup_raw_json_pipeline(local_pg, dbcp_svc, table_name="raw_bike_availability"):
    """
    配置 ReplaceText + PutSQL 管線，徹底解決 Record Schema 衝突
    """
    log("開始配置 ReplaceText + PutSQL 原生寫入管線", "STEP")
    procs = nipyapi.canvas.list_all_processors(local_pg.id)

    route_proc = next((p for p in procs if p.id == ROUTE_PROC_ID or p.component.name == ROUTE_PROC_NAME), None)
    old_putdb_proc = next((p for p in procs if p.id == PUT_DB_PROC_ID or p.component.name == PUT_DB_PROC_NAME), None)
    replace_proc = next((p for p in procs if p.component.name == REPLACE_TEXT_PROC_NAME), None)
    putsql_proc = next((p for p in procs if p.component.name == PUT_SQL_PROC_NAME or "PutSQL" in p.component.type), None)

    # 1. 停用所有涉入組件解鎖連線限制
    if route_proc:
        stop_processor_safely(route_proc)
    if old_putdb_proc:
        stop_processor_safely(old_putdb_proc)
    if replace_proc:
        stop_processor_safely(replace_proc)
    if putsql_proc:
        stop_processor_safely(putsql_proc)

    # 2. 建立或重用 PutSQL
    base_x = route_proc.component.position.x if route_proc else 0
    base_y = route_proc.component.position.y if route_proc else 0

    if not putsql_proc:
        log("建立 PutSQL 處理器...", "STEP")
        putsql_type = get_exact_processor_type("PutSQL")
        putsql_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=putsql_type,
            location=(base_x + 600, base_y),
            name=PUT_SQL_PROC_NAME
        )
    
    putsql_proc = nipyapi.canvas.get_processor(putsql_proc.id, identifier_type="id")
    if isinstance(putsql_proc, list):
        putsql_proc = putsql_proc[0]

    # 清除任何歷史無效屬性，只賦予合法屬性
    putsql_props = {
        "JDBC Connection Pool": dbcp_svc.id,
        "Support Fragmented Transactions": "false",
        "Transaction Timeout": "30 sec",
        "Batch Size": "100"
    }
    putsql_proc = apply_safe_processor_config(
        putsql_proc,
        putsql_props,
        auto_terminated_rels=["success", "failure", "retry"]
    )
    log(f"PutSQL 參數更新完成 (UUID: {putsql_proc.id})", "OK")

    # 3. 建立或重用 ReplaceText
    if not replace_proc:
        log("建立 ReplaceText 處理器...", "STEP")
        replace_type = get_exact_processor_type("ReplaceText")
        replace_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=replace_type,
            location=(base_x + 300, base_y),
            name=REPLACE_TEXT_PROC_NAME
        )

    replace_proc = nipyapi.canvas.get_processor(replace_proc.id, identifier_type="id")
    if isinstance(replace_proc, list):
        replace_proc = replace_proc[0]

    # 正確的 ReplaceText 屬性 (名稱精準對齊官方名稱 'Regular Expression')
    sql_template = f"INSERT INTO {table_name} (source_endpoint, payload) VALUES ('Bike-Availability-Taipei', \\$\\$$1\\$\\$::jsonb);"
    replace_props = {
        "Evaluation Mode": "Entire text",
        "Regular Expression": r"(?s)(.*)",
        "Replacement Value": sql_template,
        "Replacement Strategy": "Regex Replace",
        "Maximum Buffer Size": "20 MB"
    }
    replace_proc = apply_safe_processor_config(
        replace_proc,
        replace_props,
        auto_terminated_rels=["failure"]
    )
    log(f"ReplaceText 參數更新完成 (UUID: {replace_proc.id})", "OK")

    # 4. 重新接駁連線 (Route Table Type -> ReplaceText -> PutSQL)
    conns = nipyapi.canvas.list_all_connections(local_pg.id)
    
    for c in conns:
        if c.component.source.id == route_proc.id and c.component.destination.id != replace_proc.id:
            log(f"刪除直通舊連線: '{c.component.name}' (ID: {c.id})...", "INFO")
            nipyapi.canvas.delete_connection(c)

    conns = nipyapi.canvas.list_all_connections(local_pg.id)
    conn_r2t = next((c for c in conns if c.component.source.id == route_proc.id and c.component.destination.id == replace_proc.id), None)
    if not conn_r2t:
        log(f"建立連線: [{route_proc.component.name}] -> [{replace_proc.component.name}] (matched)...", "STEP")
        nipyapi.canvas.create_connection(
            source=route_proc,
            target=replace_proc,
            relationships=["matched"],
            name="Matched to SQL Wrapper"
        )

    conns = nipyapi.canvas.list_all_connections(local_pg.id)
    conn_t2s = next((c for c in conns if c.component.source.id == replace_proc.id and c.component.destination.id == putsql_proc.id), None)
    if not conn_t2s:
        log(f"建立連線: [{replace_proc.component.name}] -> [{putsql_proc.component.name}] (success)...", "STEP")
        nipyapi.canvas.create_connection(
            source=replace_proc,
            target=putsql_proc,
            relationships=["success"],
            name="Wrapped SQL to DB"
        )

    # 5. 安全刪除舊有的 PutDatabaseRecord
    if old_putdb_proc:
        try:
            log(f"移除已被取代的 PutDatabaseRecord 處理器...", "INFO")
            nipyapi.canvas.delete_processor(old_putdb_proc)
            log("舊處理器已成功移除！", "OK")
        except Exception as e:
            log(f"移除舊處理器提示: {parse_api_exception(e)}", "WARN")

    # 6. 校驗並啟動下游與上游
    for target in [replace_proc, putsql_proc]:
        for attempt in range(1, 16):
            time.sleep(0.5)
            p_fresh = nipyapi.canvas.get_processor(target.id, identifier_type="id")
            if isinstance(p_fresh, list):
                p_fresh = p_fresh[0]
            errors = p_fresh.component.validation_errors or []
            if not errors:
                log(f"處理器 [{p_fresh.component.name}] 校驗全部通過！", "OK")
                break
            if attempt == 15:
                log(f"[{p_fresh.component.name}] 仍有驗證錯誤: {errors}", "ERR")
                raise RuntimeError(f"{p_fresh.component.name} 驗證失敗: {errors}")

        start_processor_safely(target)

    if route_proc:
        start_processor_safely(route_proc)


def dump_process_group_health(local_pg):
    """輸出整體流程圖健康檢查報表，供 CI/CD 快速排查"""
    log("==========================================", "DIAG")
    log(f"=== [HEALTH-CHECK] PG: {local_pg.component.name} 狀態診斷 ===", "DIAG")
    log("==========================================", "DIAG")

    procs = nipyapi.canvas.list_all_processors(local_pg.id)
    log(f"--- Processors 列表 (共 {len(procs)} 個) ---", "DIAG")
    for p in procs:
        state_badge = "[RUNNING]" if p.component.state == "RUNNING" else f"[{p.component.state}]"
        log(f"  * {state_badge:<10} {p.component.name} (UUID: {p.id}) | 類型: {p.component.type.split('.')[-1]}", "DIAG")

    conns = nipyapi.canvas.list_all_connections(local_pg.id)
    log(f"--- Connections 列表 (共 {len(conns)} 條) ---", "DIAG")
    for c in conns:
        src = c.component.source.name
        dst = c.component.destination.name
        queue_count = c.status.aggregate_snapshot.queued_count if hasattr(c, "status") else "N/A"
        queue_size = c.status.aggregate_snapshot.queued_size if hasattr(c, "status") else "N/A"
        log(f"  * 連線: '{c.component.name}' [{src} -> {dst}] | 積壓: {queue_count} 筆 | 數據: {queue_size}", "DIAG")

    svcs = nipyapi.canvas.list_all_controllers(local_pg.id)
    log(f"--- Controller Services 列表 (共 {len(svcs)} 個) ---", "DIAG")
    for s in svcs:
        log(f"  * [{s.component.state}] {s.component.name} (UUID: {s.id})", "DIAG")
    log("==========================================", "DIAG")


def create_local_2_sql_pg(parent_pg=None, *args, **kwargs):
    """主進入點函式"""
    log("==========================================", "STEP")
    log("=== [START] Local_2_SQL 流程圖配置部署 ===", "STEP")
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
            raise ValueError(f"找不到 Process Group: {PROCESS_GROUP_NAME} ({PROCESS_GROUP_ID})")

        log(f"鎖定目標 Process Group: {local_pg.component.name} (UUID: {local_pg.id})", "OK")

        # 1. 確保 DBCP 服務啟動
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

        # 2. 建置 ReplaceText + PutSQL 正式管線
        table_name = kwargs.get("table_name", "raw_bike_availability")
        setup_raw_json_pipeline(local_pg, dbcp_svc, table_name=table_name)

        # 3. 輸出全局健康檢查報告
        dump_process_group_health(local_pg)

        log("==========================================", "OK")
        log("=== [FINISH] Local_2_SQL 部署全部順利完成 ===", "OK")
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