#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - Local_2_SQL 流程圖模組 (Prepend + Append 零正則保證成功版)
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

PREPEND_PROC_NAME = "Wrap SQL Header (Prepend)"
APPEND_PROC_NAME = "Wrap SQL Footer (Append)"
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
    """先清空佇列殘留數據，再安全刪除連線，徹底避開 409 Conflict"""
    if not conn:
        return
    try:
        # 清空佇列中的 FlowFile
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
    """
    屬性精確對齊與幽靈屬性清理：
    先將所有舊屬性標記為 None (觸發伺服器端刪除)，再寫入合法白名單鍵值。
    """
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

    # 若屬性無差異且已經 ENABLED，直接沿用
    if svc.component.state == "ENABLED":
        log(f"Controller Service 已處於 ENABLED 狀態，直接沿用。", "OK")
        return svc

    # 啟用服務
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


def setup_prepend_append_pipeline(local_pg, dbcp_svc, table_name="raw_bike_availability"):
    """
    【Prepend + Append 零正則保證成功架構】
    1. 清理所有舊 ReplaceText 處理器與積壓連線。
    2. 建立 Prepend 處理器：寫入 SQL Header 與 Dollar-Quote 開頭 ($raw_json$)。
    3. 建立 Append 處理器：寫入 Dollar-Quote 結尾 ($raw_json$::jsonb);。
    4. 連線至 PutSQL 寫入資料庫。
    """
    log("開始執行 Prepend + Append 零正則架構部署...", "STEP")
    procs = nipyapi.canvas.list_all_processors(local_pg.id)

    route_proc = next((p for p in procs if p.id == ROUTE_PROC_ID or p.component.name == ROUTE_PROC_NAME), None)
    old_putdb_list = [p for p in procs if "PutDatabaseRecord" in p.component.name or "PutDatabaseRecord" in p.component.type]
    old_replace_list = [p for p in procs if "ReplaceText" in p.component.type]
    putsql_proc = next((p for p in procs if p.component.name == PUT_SQL_PROC_NAME or "PutSQL" in p.component.type), None)

    # 1. 安全停止所有涉及的處理器
    log("停止所有涉入處理器以解鎖連線與佇列...", "STEP")
    for p in [route_proc, putsql_proc] + old_replace_list + old_putdb_list:
        if p:
            stop_processor_safely(p)

    # 2. 徹底清空並刪除下游所有連線 (解鎖卡在隊列中的 506 KB 舊資料)
    conns = nipyapi.canvas.list_all_connections(local_pg.id)
    replace_ids = [p.id for p in old_replace_list]
    putdb_ids = [p.id for p in old_putdb_list]
    putsql_id = putsql_proc.id if putsql_proc else None

    for c in conns:
        src_id = c.component.source.id
        dst_id = c.component.destination.id
        if src_id in replace_ids or src_id in putdb_ids or \
           dst_id in replace_ids or dst_id in putdb_ids or dst_id == putsql_id:
            safe_purge_and_delete_connection(c)

    # 3. 刪除所有舊有的 ReplaceText 與 PutDatabaseRecord 節點
    for p in old_replace_list:
        log(f"刪除舊 ReplaceText 實例 (ID: {p.id})...", "INFO")
        try:
            nipyapi.canvas.delete_processor(p)
        except Exception as e:
            log(f"刪除舊處理器提示: {parse_api_exception(e)}", "WARN")

    for p in old_putdb_list:
        try:
            nipyapi.canvas.delete_processor(p)
        except Exception as e:
            log(f"刪除舊處理器提示: {parse_api_exception(e)}", "WARN")

    # 4. 座標佈局
    base_x = route_proc.component.position.x if route_proc else 0
    base_y = route_proc.component.position.y if route_proc else 0

    # 5. 配置 PutSQL 處理器
    if not putsql_proc:
        log("建立 PutSQL 處理器...", "STEP")
        putsql_type = get_exact_processor_type("PutSQL")
        putsql_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=putsql_type,
            location=(base_x + 900, base_y),
            name=PUT_SQL_PROC_NAME
        )

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
    log(f"PutSQL 配置完成 (UUID: {putsql_proc.id})", "OK")

    # 6. 建立 Prepend 處理器 (寫入 SQL 開頭)
    log("建立 Prepend 處理器 (寫入 SQL 開頭)...", "STEP")
    replace_type = get_exact_processor_type("ReplaceText")
    prepend_proc = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=replace_type,
        location=(base_x + 300, base_y),
        name=PREPEND_PROC_NAME
    )
    prepend_proc = nipyapi.canvas.get_processor(prepend_proc.id, identifier_type="id")
    if isinstance(prepend_proc, list):
        prepend_proc = prepend_proc[0]

    # Prepend 策略：直接在檔案開頭貼上文字，無任何正規表達式
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
    log(f"Prepend 處理器配置完成 (UUID: {prepend_proc.id})", "OK")

    # 7. 建立 Append 處理器 (寫入 SQL 結尾)
    log("建立 Append 處理器 (寫入 SQL 結尾)...", "STEP")
    append_proc = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=replace_type,
        location=(base_x + 600, base_y),
        name=APPEND_PROC_NAME
    )
    append_proc = nipyapi.canvas.get_processor(append_proc.id, identifier_type="id")
    if isinstance(append_proc, list):
        append_proc = append_proc[0]

    # Append 策略：直接在檔案末端貼上結尾，無任何正規表達式
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
    log(f"Append 處理器配置完成 (UUID: {append_proc.id})", "OK")

    # 8. 接駁連線: Route -> Prepend -> Append -> PutSQL
    log("接駁連線: [Route Table Type] -> [Prepend] (matched)...", "STEP")
    nipyapi.canvas.create_connection(
        source=route_proc,
        target=prepend_proc,
        relationships=["matched"],
        name="Matched to Prepend"
    )

    log("接駁連線: [Prepend] -> [Append] (success)...", "STEP")
    nipyapi.canvas.create_connection(
        source=prepend_proc,
        target=append_proc,
        relationships=["success"],
        name="Prepended to Append"
    )

    log("接駁連線: [Append] -> [PutSQL] (success)...", "STEP")
    nipyapi.canvas.create_connection(
        source=append_proc,
        target=putsql_proc,
        relationships=["success"],
        name="SQL Complete to DB"
    )

    # 9. 輪詢驗證與按順序啟動
    for target in [putsql_proc, append_proc, prepend_proc]:
        log(f"等待處理器 [{target.component.name}] 通過 NiFi 校驗...", "INFO")
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
                raise RuntimeError(f"{p_fresh.component.name} 驗證失敗: {errors}")

        start_processor_safely(target)

    # 恢復上游 Route 處理器運作
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

        # 2. 部署 Prepend + Append 零正則保證架構
        table_name = kwargs.get("table_name", "raw_bike_availability")
        setup_prepend_append_pipeline(local_pg, dbcp_svc, table_name=table_name)

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