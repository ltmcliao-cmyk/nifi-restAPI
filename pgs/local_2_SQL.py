#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - Local_2_SQL 流程圖模組 (具備豐富排查日誌與連線自動重配版)
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
PUT_DB_PROC_ID = "10d78562-01a1-1000-adec-5b289ce54b88"
PUT_DB_PROC_NAME = "PutDatabaseRecord to PostgreSQL"
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
    """解析 NiFi API Exception 的深層錯誤資訊以供排查"""
    err_details = {"status": getattr(e, "status", "N/A"), "reason": getattr(e, "reason", "N/A")}
    body = getattr(e, "body", None)
    if body:
        try:
            err_details["body"] = json.loads(body)
        except Exception:
            err_details["body"] = str(body)
    return err_details


def stop_processor_safely(proc):
    """安全停止指定處理器，並等待至 STOPPED 狀態"""
    if not proc:
        return
    proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    log(f"檢查處理器狀態: '{proc.component.name}' (UUID: {proc.id}) -> 當前狀態: {proc.component.state}", "DIAG")
    if proc.component.state == "RUNNING":
        log(f"處理器執行中，正在發送停止指令以解除鎖定...", "STEP")
        try:
            nipyapi.canvas.schedule_processor(proc, scheduled=False)
        except Exception as e:
            log(f"停止處理器時收到警告: {parse_api_exception(e)}", "WARN")

        for attempt in range(1, 16):
            time.sleep(0.5)
            proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
            if isinstance(proc, list):
                proc = proc[0]
            if proc.component.state == "STOPPED":
                log(f"處理器 '{proc.component.name}' 已成功轉為 STOPPED！", "OK")
                return
        log(f"處理器停止逾時 (當前狀態: {proc.component.state})", "WARN")


def ensure_local_controller_service(local_pg, service_type, service_name, properties=None):
    """建立或更新 Controller Service，含連線依賴衝突排查與防護"""
    log(f"檢查 Controller Service: '{service_name}'", "STEP")
    services = nipyapi.canvas.list_all_controllers(local_pg.id)
    svc = next((s for s in services if s.component.name == service_name), None)

    if not svc:
        log(f"服務不存在，在 Process Group [{local_pg.id}] 中建立: {service_name} ({service_type})", "INFO")
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

    # 比對連線屬性是否需變動
    needs_update = False
    if properties:
        current_props = svc.component.properties or {}
        diffs = {k: (current_props.get(k), v) for k, v in properties.items() if current_props.get(k) != v}
        if diffs:
            needs_update = True
            log(f"服務屬性差異比對結果: {diffs}", "DIAG")

    if needs_update:
        if svc.component.state != "DISABLED":
            log(f"正在停用 {service_name} 以便寫入新屬性...", "INFO")
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
            log(f"正在推送新配置至 {service_name}...", "STEP")
            current_props = svc.component.properties or {}
            current_props.update(properties)
            svc.component.properties = current_props
            svc = nipyapi.canvas.update_controller(svc, svc.component)
            log(f"屬性更新成功！", "OK")
        else:
            log(f"服務當前狀態為 {svc.component.state}，略過屬性更新以防 400 錯誤", "WARN")
    else:
        log(f"屬性與現有配置完全一致，略過更新動作以維持穩定", "DIAG")

    # 確保進入 ENABLED 狀態
    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    if svc.component.state != "ENABLED":
        log(f"發送啟用命令至 {service_name}...", "STEP")
        try:
            nipyapi.canvas.schedule_controller(svc, scheduled=True)
        except Exception as e:
            log(f"啟用排程提示: {parse_api_exception(e)}", "WARN")

        for attempt in range(1, 21):
            time.sleep(0.5)
            svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
            if isinstance(svc, list):
                svc = svc[0]
            if svc.component.state == "ENABLED":
                log(f"Controller Service '{service_name}' 順利轉為 ENABLED！", "OK")
                break
        
        if svc.component.state != "ENABLED":
            errors = svc.component.validation_errors or []
            log(f"Controller Service 啟用失敗！驗證錯誤: {errors}", "ERR")
            raise RuntimeError(f"Controller Service {service_name} 無法啟用: {errors}")

    return svc


def rewire_connection_to_target(local_pg, old_proc_id, new_proc):
    """
    排查並安全重導連線：優先使用 ConnectionsApi，若失敗則透過元數據重建
    """
    conns = nipyapi.canvas.list_all_connections(local_pg.id)
    incoming_conns = [c for c in conns if c.component.destination.id == old_proc_id]

    if not incoming_conns:
        log(f"未偵測到指向舊處理器 ({old_proc_id}) 的進線連線", "DIAG")
        return

    for conn in incoming_conns:
        conn_name = conn.component.name or "Unnamed-Connection"
        source_id = conn.component.source.id
        source_name = conn.component.source.name
        rels = conn.component.selected_relationships or []
        queued_count = conn.status.aggregate_snapshot.queued_count if hasattr(conn, "status") else "0"
        
        log(f"發現目標連線: '{conn_name}' (ID: {conn.id}) [來源: {source_name} -> 關係: {rels} | 佇列: {queued_count} 筆]", "DIAG")

        # 方式 1: 透過 ConnectionsApi 原地修改 Destination
        success = False
        try:
            log(f"嘗試透過 ConnectionsApi 重導目的端至新處理器 (UUID: {new_proc.id})...", "STEP")
            conn_fresh = nipyapi.canvas.get_connection(conn.id, identifier_type="id")
            if isinstance(conn_fresh, list):
                conn_fresh = conn_fresh[0]
            conn_fresh.component.destination.id = new_proc.id
            conn_fresh.component.destination.type = "PROCESSOR"
            conn_fresh.component.destination.group_id = local_pg.id
            
            nipyapi.nifi.ConnectionsApi().update_connection(id=conn_fresh.id, body=conn_fresh)
            log(f"連線 '{conn_name}' 成功原地更新目的端！", "OK")
            success = True
        except Exception as e:
            log(f"原地更新連線失敗 ({parse_api_exception(e)})，切換為『刪除並重連』策略...", "WARN")

        # 方式 2: 容錯機制 (刪除舊連線並建立新連線)
        if not success:
            source_proc = nipyapi.canvas.get_processor(source_id, identifier_type="id")
            if isinstance(source_proc, list):
                source_proc = source_proc[0]

            log(f"刪除舊連線 (ID: {conn.id})...", "INFO")
            nipyapi.canvas.delete_connection(conn)

            log(f"重新建立連線: [{source_proc.component.name}] -> [{new_proc.component.name}] (關係: {rels})...", "STEP")
            new_conn = nipyapi.canvas.create_connection(
                source=source_proc,
                target=new_proc,
                relationships=rels,
                name=conn_name
            )
            log(f"新連線建立成功，UUID: {new_conn.id}", "OK")


def switch_to_putsql_processor(local_pg, dbcp_svc, table_name="raw_bike_availability"):
    """
    將原處理器轉換/替換為 PutSQL，直接將整份 FlowFile 寫入 payload JSONB
    """
    log("開始配置 PutSQL 處理器 (RAW JSONB 寫入模式)", "STEP")
    
    # 尋找現有的舊處理器與可能已經建立過的 PutSQL
    procs = nipyapi.canvas.list_all_processors(local_pg.id)
    old_proc = next((p for p in procs if p.id == PUT_DB_PROC_ID or p.component.name == PUT_DB_PROC_NAME), None)
    existing_putsql = next((p for p in procs if p.component.name == PUT_SQL_PROC_NAME or "PutSQL" in p.component.type), None)

    # 確保停止舊處理器
    if old_proc:
        stop_processor_safely(old_proc)

    target_proc = None
    if existing_putsql:
        log(f"找到已存在的 PutSQL 處理器: '{existing_putsql.component.name}' (UUID: {existing_putsql.id})，直接重用", "INFO")
        target_proc = existing_putsql
    else:
        pos_x = old_proc.component.position.x if old_proc else 0
        pos_y = old_proc.component.position.y if old_proc else 0
        log(f"建立新的 PutSQL 處理器於座標 ({pos_x}, {pos_y})...", "STEP")
        target_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=nipyapi.canvas.get_processor_type("PutSQL"),
            location=(pos_x, pos_y),
            name=PUT_SQL_PROC_NAME
        )
        log(f"PutSQL 建立完成，UUID: {target_proc.id}", "OK")

    # 若舊處理器仍存在，重導連線並將其移除
    if old_proc and old_proc.id != target_proc.id:
        rewire_connection_to_target(local_pg, old_proc.id, target_proc)
        log(f"準備移除已停止且無連線的舊處理器: '{old_proc.component.name}'...", "INFO")
        try:
            nipyapi.canvas.delete_processor(old_proc)
            log("舊處理器已安全移除！", "OK")
        except Exception as e:
            log(f"移除舊處理器提示: {parse_api_exception(e)}", "WARN")

    # 配置 PutSQL 屬性 (寫入 raw_bike_availability 的 payload JSONB)
    sql_statement = f"INSERT INTO {table_name} (source_endpoint, payload) VALUES ('Bike-Availability-Taipei', ?::jsonb)"
    log(f"設定 SQL 語句: {sql_statement}", "DIAG")

    target_proc = nipyapi.canvas.get_processor(target_proc.id, identifier_type="id")
    if isinstance(target_proc, list):
        target_proc = target_proc[0]

    props = {
        "JDBC Connection Pool": dbcp_svc.id,
        "SQL Statement": sql_statement,
        "Support Fragmented Transactions": "false",
        "Transaction Timeout": "30 sec",
        "Batch Size": "100"
    }

    target_proc.component.config.properties = props
    target_proc.component.config.auto_terminated_relationships = ["success", "failure", "retry"]

    log("提交 PutSQL 屬性配置至 NiFi API...", "STEP")
    updated_proc = nipyapi.canvas.update_processor(target_proc, target_proc.component.config)

    # 驗證檢查
    log("等待 NiFi 校驗引擎完成處理器設定確認...", "INFO")
    for attempt in range(1, 16):
        time.sleep(0.5)
        updated_proc = nipyapi.canvas.get_processor(updated_proc.id, identifier_type="id")
        if isinstance(updated_proc, list):
            updated_proc = updated_proc[0]

        errors = updated_proc.component.validation_errors or []
        log(f"[校驗檢查 {attempt}/15] 錯誤數: {len(errors)}", "DIAG")
        if not errors:
            log("PutSQL 所有配置驗證完全通過！", "OK")
            break

    errors = updated_proc.component.validation_errors or []
    if errors:
        log(f"PutSQL 仍有殘留驗證錯誤: {errors}", "ERR")
        for err in errors:
            log(f"  * {err}", "ERR")
        raise RuntimeError(f"PutSQL 驗證失敗: {errors}")

    # 啟動處理器
    if updated_proc.component.state == "STOPPED":
        log("發送啟動指令至 PutSQL 處理器...", "STEP")
        try:
            nipyapi.canvas.schedule_processor(updated_proc, scheduled=True)
            log("PutSQL 處理器啟動命令已下達！", "OK")
        except Exception as e:
            log(f"排程啟動異常: {parse_api_exception(e)}", "WARN")

    return updated_proc


def dump_process_group_health(local_pg):
    """輸出整體流程圖健康檢查報表，供 CI/CD 快速排查"""
    log("==========================================", "DIAG")
    log(f"=== [HEALTH-CHECK] PG: {local_pg.component.name} 狀態診斷 ===", "DIAG")
    log("==========================================", "DIAG")
    
    # 1. 處理器狀態
    procs = nipyapi.canvas.list_all_processors(local_pg.id)
    log(f"--- Processors 列表 (共 {len(procs)} 個) ---", "DIAG")
    for p in procs:
        state_badge = "[RUNNING]" if p.component.state == "RUNNING" else f"[{p.component.state}]"
        log(f"  * {state_badge:<10} {p.component.name} (UUID: {p.id}) | 類型: {p.component.type.split('.')[-1]}", "DIAG")

    # 2. 連線與佇列狀態
    conns = nipyapi.canvas.list_all_connections(local_pg.id)
    log(f"--- Connections 列表 (共 {len(conns)} 條) ---", "DIAG")
    for c in conns:
        src = c.component.source.name
        dst = c.component.destination.name
        queue_count = c.status.aggregate_snapshot.queued_count if hasattr(c, "status") else "N/A"
        queue_size = c.status.aggregate_snapshot.queued_size if hasattr(c, "status") else "N/A"
        log(f"  * 連線: '{c.component.name}' [{src} -> {dst}] | 積壓數量: {queue_count} | 數據量: {queue_size}", "DIAG")

    # 3. 控制服務狀態
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

        # 1. 前置安全檢查：優先停用舊的 PutDatabaseRecord 處理器避免佔用 DBCP
        old_proc = nipyapi.canvas.get_processor(PUT_DB_PROC_ID, identifier_type="id")
        if isinstance(old_proc, list) and old_proc:
            stop_processor_safely(old_proc[0])
        elif old_proc:
            stop_processor_safely(old_proc)

        # 2. 確保 DBCP 服務啟動
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

        # 3. 部署 / 替換為 PutSQL 並重導連線
        table_name = kwargs.get("table_name", "raw_bike_availability")
        switch_to_putsql_processor(local_pg, dbcp_svc, table_name=table_name)

        # 4. 輸出全局健康檢查報告
        dump_process_group_health(local_pg)

        log("==========================================", "OK")
        log("=== [FINISH] Local_2_SQL 部署全部順利完成 ===", "OK")
        log("==========================================", "OK")
        return local_pg

    except Exception as e:
        log(f"部署過程發生致命例外: {e}", "ERR")
        traceback.print_exc()
        raise e


if __name__ == "__main__":
    nipyapi.config.nifi_config.host = "http://127.0.0.1:8080/nifi-api"
    pg = create_local_2_sql_pg()
    try:
        nipyapi.canvas.schedule_process_group(pg.id, scheduled=True)
    except Exception as e:
        log(f"schedule_process_group 提示: {parse_api_exception(e)}", "WARN")