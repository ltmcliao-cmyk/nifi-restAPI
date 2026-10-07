#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - Local_2_SQL 流程圖模組 (Clean-Slate 零殘留與全防禦寫入終極版)
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
    """精準提取單一 DocumentedTypeDTO 實例，杜絕型別解析錯誤"""
    log(f"向 NiFi 查詢處理器型別: '{type_name}'...", "DIAG")
    entity = nipyapi.nifi.FlowApi().get_processor_types()
    types_list = getattr(entity, "processor_types", [])

    for t in types_list:
        if t.type == type_name or t.type.endswith(f".{type_name}"):
            log(f"型別精確鎖定成功 -> {t.type}", "OK")
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

    log(f"檢查處理器狀態: '{proc.component.name}' (UUID: {proc.id}) -> 當前: {proc.component.state}", "DIAG")
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
        log(f"處理器停止逾時 (狀態: {proc.component.state})", "WARN")


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


def safe_delete_connection(conn):
    """安全清空並刪除連線，防止殘留數據引發 409"""
    if not conn:
        return
    try:
        # 若有殘留佇列，先嘗試 drop flowfiles
        queue_count = getattr(getattr(conn, "status", None), "aggregate_snapshot", None)
        if queue_count and getattr(queue_count, "queued_count", 0) > 0:
            log(f"清空連線佇列殘留數據 (ID: {conn.id})...", "WARN")
            nipyapi.nifi.FlowfileQueuesApi().create_drop_request(conn.id)
            time.sleep(1)

        nipyapi.canvas.delete_connection(conn)
        log(f"連線已安全刪除 (ID: {conn.id})", "OK")
    except Exception as e:
        log(f"連線刪除警告: {parse_api_exception(e)}", "WARN")


def configure_processor_clean(proc, desired_properties, auto_terminated_rels=None):
    """
    動態屬性白名單校準函式：
    將既有不在白名單的幽靈屬性全部明確標記為 None (null)，強制 NiFi 伺服器徹底抹除。
    """
    descriptors = proc.component.config.descriptors or {}
    current_props = proc.component.config.properties or {}
    clean_props = {}

    # 1. 凡是目前存在於處理器上的鍵，預設全部標記為 None (強制刪除)
    for old_k in current_props.keys():
        clean_props[old_k] = None

    # 2. 依據 Descriptors 規格精準注入合法鍵
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
            log(f"[{proc.component.name}] 屬性對齊: '{target_name}' -> Real Key: '{matched_key}'", "DIAG")
        else:
            log(f"[{proc.component.name}] 警告: 屬性 '{target_name}' 不存在於官方規格中！", "WARN")

    proc.component.config.properties = clean_props

    if auto_terminated_rels:
        valid_rels = [r.name for r in proc.component.relationships]
        proc.component.config.auto_terminated_relationships = [
            r for r in auto_terminated_rels if r in valid_rels
        ]

    return nipyapi.canvas.update_processor(proc, proc.component.config)


def ensure_local_controller_service(local_pg, service_type, service_name, properties=None):
    """建立或確認 Controller Service 狀態，維持冪等性"""
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

    needs_update = False
    if properties:
        current_props = svc.component.properties or {}
        diffs = {k: (current_props.get(k), v) for k, v in properties.items() if current_props.get(k) != v}
        if diffs:
            needs_update = True
            log(f"連線池屬性有差異需更新: {diffs}", "DIAG")

    if needs_update:
        if svc.component.state != "DISABLED":
            log(f"暫停 {service_name} 以便覆寫連線配置...", "INFO")
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
            log(f"連線屬性更新成功！", "OK")

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


def setup_raw_json_pipeline_clean(local_pg, dbcp_svc, table_name="raw_bike_availability"):
    """
    【Clean-Slate 終極重建架構】
    1. 停用鏈路上所有處理器。
    2. 拆除所有通往下游的連線。
    3. 徹底刪除帶有殘留屬性報錯的舊 ReplaceText 處理器。
    4. 建立全新的 ReplaceText 與 PutSQL 處理器，確保 0 幽靈屬性殘留。
    5. 重接 Route -> ReplaceText -> PutSQL 連線並進行驗證啟動。
    """
    log("開始執行 Clean-Slate 乾淨架構部署...", "STEP")
    procs = nipyapi.canvas.list_all_processors(local_pg.id)

    route_proc = next((p for p in procs if p.id == ROUTE_PROC_ID or p.component.name == ROUTE_PROC_NAME), None)
    old_putdb = next((p for p in procs if "PutDatabaseRecord" in p.component.name or "PutDatabaseRecord" in p.component.type), None)
    old_replace = next((p for p in procs if p.component.name == REPLACE_TEXT_PROC_NAME), None)
    old_putsql = next((p for p in procs if p.component.name == PUT_SQL_PROC_NAME), None)

    # 1. 安全全面停止，解鎖所有 409 連線限制
    for p in [route_proc, old_putdb, old_replace, old_putsql]:
        if p:
            stop_processor_safely(p)

    # 2. 徹底拆除所有舊連線，防止殘留
    conns = nipyapi.canvas.list_all_connections(local_pg.id)
    for c in conns:
        src_id = c.component.source.id
        dst_id = c.component.destination.id
        # 只要涉及下游客製寫入節點的連線一律拆除重建
        if src_id in [getattr(old_replace, 'id', None), getattr(old_putdb, 'id', None)] or \
           dst_id in [getattr(old_replace, 'id', None), getattr(old_putsql, 'id', None), getattr(old_putdb, 'id', None)]:
            log(f"拆除舊連線: '{c.component.name}' (ID: {c.id})...", "INFO")
            safe_delete_connection(c)

    # 3. 徹底刪除可能被污染的舊實例
    if old_replace:
        log(f"徹底移除舊有帶有殘留配置的 ReplaceText (UUID: {old_replace.id})...", "STEP")
        try:
            nipyapi.canvas.delete_processor(old_replace)
            log("舊 ReplaceText 已成功粉碎清理！", "OK")
        except Exception as e:
            log(f"移除舊 ReplaceText 提示: {parse_api_exception(e)}", "WARN")

    if old_putdb:
        log(f"移除舊有的 PutDatabaseRecord...", "INFO")
        try:
            nipyapi.canvas.delete_processor(old_putdb)
        except Exception as e:
            log(f"移除舊 PutDatabaseRecord 提示: {parse_api_exception(e)}", "WARN")

    # 4. 座標計算與全新建立
    base_x = route_proc.component.position.x if route_proc else 0
    base_y = route_proc.component.position.y if route_proc else 0

    # 處理 PutSQL
    if not old_putsql:
        log("建立全新的 PutSQL 處理器...", "STEP")
        putsql_type = get_exact_processor_type("PutSQL")
        putsql_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=putsql_type,
            location=(base_x + 600, base_y),
            name=PUT_SQL_PROC_NAME
        )
    else:
        putsql_proc = old_putsql

    putsql_proc = nipyapi.canvas.get_processor(putsql_proc.id, identifier_type="id")
    if isinstance(putsql_proc, list):
        putsql_proc = putsql_proc[0]

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
    log(f"PutSQL 乾淨配置更新完成 (UUID: {putsql_proc.id})", "OK")

    # 建立全新的 ReplaceText (保證 0 幽靈屬性)
    log("建立全新的 ReplaceText 處理器 (Clean-Slate)...", "STEP")
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

    # 使用 PostgreSQL Dollar-Quoting 標籤 $json$...$json$::jsonb
    sql_template = f"INSERT INTO {table_name} (source_endpoint, payload) VALUES ('Bike-Availability-Taipei', \\$json\\$$1\\$json\\$::jsonb)"
    replace_props = {
        "Evaluation Mode": "Entire text",
        "Regular Expression": r"(?s)(.*)",
        "Replacement Value": sql_template,
        "Replacement Strategy": "Regex Replace",
        "Maximum Buffer Size": "20 MB"
    }
    replace_proc = configure_processor_clean(
        replace_proc,
        replace_props,
        auto_terminated_rels=["failure"]
    )
    log(f"全新 ReplaceText 配置完成 (UUID: {replace_proc.id})", "OK")

    # 5. 接駁全新連線鏈路
    log("接駁全新鏈路: [Route Table Type] -> [ReplaceText] (matched)...", "STEP")
    nipyapi.canvas.create_connection(
        source=route_proc,
        target=replace_proc,
        relationships=["matched"],
        name="Matched to SQL Wrapper"
    )

    log("接駁全新鏈路: [ReplaceText] -> [PutSQL] (success)...", "STEP")
    nipyapi.canvas.create_connection(
        source=replace_proc,
        target=putsql_proc,
        relationships=["success"],
        name="Wrapped SQL to DB"
    )

    # 6. 漸進輪詢校驗與啟動
    for target in [replace_proc, putsql_proc]:
        log(f"等待處理器 [{target.component.name}] 通過 NiFi 核心校驗...", "INFO")
        for attempt in range(1, 21):
            time.sleep(0.5)
            p_fresh = nipyapi.canvas.get_processor(target.id, identifier_type="id")
            if isinstance(p_fresh, list):
                p_fresh = p_fresh[0]
            errors = p_fresh.component.validation_errors or []
            if not errors:
                log(f"處理器 [{p_fresh.component.name}] 校驗全部通過！", "OK")
                break
            if attempt == 20:
                log(f"[{p_fresh.component.name}] 仍有殘留驗證錯誤: {errors}", "ERR")
                raise RuntimeError(f"{p_fresh.component.name} 驗證失敗: {errors}")

        start_processor_safely(target)

    # 恢復上游 Route 處理器
    if route_proc:
        start_processor_safely(route_proc)


def fetch_latest_bulletins(local_pg):
    """抓取最新 Bulletin 日誌，協助排查資料庫寫入細節"""
    log("查詢 NiFi 最新 Bulletins (即時日誌)...", "DIAG")
    try:
        bulletin_board = nipyapi.nifi.FlowApi().get_bulletin_board()
        bulletins = getattr(bulletin_board.bulletin_board, "bulletins", []) or []
        local_bulletins = [b for b in bulletins if getattr(b, "group_id", None) == local_pg.id]

        if not local_bulletins:
            log("當前 Process Group 無任何異常 Bulletin 警告！", "OK")
            return

        log(f"發現 {len(local_bulletins)} 筆 Bulletin 訊息:", "WARN")
        for b in local_bulletins[-5:]:
            src = getattr(b, "source_name", "Unknown Source")
            level = getattr(b, "level", "INFO")
            msg = getattr(b, "message", "")
            log(f"  [{level}] 來源: {src} -> 內容: {msg}", "DIAG")
    except Exception as e:
        log(f"抓取 Bulletin 提示: {e}", "WARN")


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

        # 2. 執行乾淨架構部署
        table_name = kwargs.get("table_name", "raw_bike_availability")
        setup_raw_json_pipeline_clean(local_pg, dbcp_svc, table_name=table_name)

        # 3. 輸出全局健康檢查報告與 Bulletin 排查日誌
        dump_process_group_health(local_pg)
        fetch_latest_bulletins(local_pg)

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