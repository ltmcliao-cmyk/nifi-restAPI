#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - Local_2_SQL 流程圖模組 (改用 PutSQL 原生寫入 RAW JSONB 修正版)
"""

import os
import sys
import time
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

DEFAULT_DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/your_database",
    "driver_class": "org.postgresql.Driver",
    "driver_location": "/opt/nifi/drivers/postgresql-42.2.14.jar",
    "user": "postgres",
    "password": "your_password",
}


def log(msg):
    """統一的 CI/CD 格式化日誌輸出"""
    print(f"[DEPLOY-LOG {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def ensure_local_controller_service(local_pg, service_type, service_name, properties=None):
    """在 Local PG 建立並啟用 DBCP Controller Service"""
    log(f"--- 檢查 Controller Service: {service_name} ---")
    services = nipyapi.canvas.list_all_controllers(local_pg.id)
    svc = next((s for s in services if s.component.name == service_name), None)

    if not svc:
        log(f"-> 服務不存在，正在於 PG [{local_pg.id}] 內部建立: {service_name}...")
        svc = nipyapi.canvas.create_controller(
            parent_pg=local_pg,
            controller_type=service_type,
            name=service_name
        )
        log(f"-> 建立成功，UUID: {svc.id}")
    else:
        log(f"-> 找到現有服務 UUID: {svc.id}，當前狀態: {svc.component.state}")

    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    # 更新屬性
    if properties:
        if svc.component.state == "ENABLED":
            log(f"-> 暫停 {service_name} 以便更新連線屬性...")
            try:
                nipyapi.canvas.schedule_controller(svc, scheduled=False)
            except Exception as e:
                log(f"   [WARN] 暫停服務提示: {e}")
            time.sleep(1)
            svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
            if isinstance(svc, list):
                svc = svc[0]

        log(f"-> 正在更新 {service_name} 屬性...")
        current_props = svc.component.properties or {}
        current_props.update(properties)
        svc.component.properties = current_props
        svc = nipyapi.canvas.update_controller(svc, svc.component)

    # 啟用服務
    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    if svc.component.state != "ENABLED":
        log(f"-> 發送啟用指令至 {service_name}...")
        try:
            nipyapi.canvas.schedule_controller(svc, scheduled=True)
        except Exception as e:
            log(f"   [WARN] 啟用調度提示: {e}")

        for attempt in range(1, 21):
            time.sleep(0.5)
            svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
            if isinstance(svc, list):
                svc = svc[0]
            if svc.component.state == "ENABLED":
                log(f"[SUCCESS] {service_name} 已成功轉為 ENABLED！")
                break

    return svc


def switch_to_putsql_processor(local_pg, dbcp_svc, table_name="raw_bike_availability"):
    """
    將原處理器轉換/替換為 PutSQL，直接將整份 FlowFile 寫入 payload JSONB
    """
    log("--- 開始設定 PutSQL 處理器 (RAW JSONB 寫入) ---")
    proc = nipyapi.canvas.get_processor(PUT_DB_PROC_ID, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    if not proc:
        proc = nipyapi.canvas.get_processor(PUT_DB_PROC_NAME, identifier_type="name")
        if isinstance(proc, list) and proc:
            proc = proc[0]

    # 如果舊處理器存在且是 RUNNING，先將其停止
    if proc:
        proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
        if isinstance(proc, list):
            proc = proc[0]
        if proc.component.state == "RUNNING":
            log("-> 停止舊的寫入處理器...")
            try:
                nipyapi.canvas.schedule_processor(proc, scheduled=False)
                time.sleep(1)
            except Exception as e:
                log(f"[WARN] 停止處理器提示: {e}")

    # 檢查是否需要重新建立為標準 PutSQL
    is_putsql = proc and "PutSQL" in proc.component.type
    if not is_putsql and proc:
        log("-> 偵測到舊處理器為 PutDatabaseRecord，準備替換為 PutSQL...")
        # 取得連線資訊以便重建
        conns = nipyapi.canvas.list_all_connections(local_pg.id)
        incoming_conn = next((c for c in conns if c.destination_id == proc.id), None)

        # 建立 PutSQL
        new_proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=nipyapi.canvas.get_processor_type("PutSQL"),
            location=(proc.component.position.x, proc.component.position.y),
            name="PutSQL to PostgreSQL RAW"
        )
        log(f"-> 新增 PutSQL 成功，UUID: {new_proc.id}")

        # 若有前置連線，重定向至新處理器
        if incoming_conn:
            log(f"-> 重定向連線 [{incoming_conn.component.name}] 到新的 PutSQL...")
            incoming_conn.component.destination.id = new_proc.id
            nipyapi.canvas.update_connection(incoming_conn)

        # 刪除舊處理器
        try:
            nipyapi.canvas.delete_processor(proc)
            log("-> 舊處理器已移除")
        except Exception as e:
            log(f"[WARN] 移除舊處理器提示: {e}")

        proc = new_proc

    elif not proc:
        # 直接於流程圖建立 PutSQL
        log("-> 直接建立 PutSQL 處理器...")
        proc = nipyapi.canvas.create_processor(
            parent_pg=local_pg,
            processor=nipyapi.canvas.get_processor_type("PutSQL"),
            location=(0, 0),
            name="PutSQL to PostgreSQL RAW"
        )

    proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    # 設定 SQL 寫入語法與 DBCP Pool
    sql_statement = f"INSERT INTO {table_name} (source_endpoint, payload) VALUES ('Bike-Availability-Taipei', ?::jsonb)"
    
    props = {
        "JDBC Connection Pool": dbcp_svc.id,
        "SQL Statement": sql_statement,
        "Support Fragmented Transactions": "false",
        "Transaction Timeout": "30 sec",
        "Batch Size": "100"
    }

    proc.component.config.properties = props
    proc.component.config.auto_terminated_relationships = ["success", "failure", "retry"]

    log("-> 提交 PutSQL 參數配置至 NiFi...")
    updated_proc = nipyapi.canvas.update_processor(proc, proc.component.config)

    # 驗證狀態
    log("-> 等待 NiFi 驗證清除所有配置錯誤...")
    for attempt in range(1, 16):
        time.sleep(0.5)
        updated_proc = nipyapi.canvas.get_processor(updated_proc.id, identifier_type="id")
        if isinstance(updated_proc, list):
            updated_proc = updated_proc[0]

        errors = updated_proc.component.validation_errors or []
        log(f"   [校驗檢查 {attempt}/15] 錯誤數量: {len(errors)}")
        if not errors:
            log("[SUCCESS] PutSQL 校驗完全通過！")
            break

    errors = updated_proc.component.validation_errors or []
    if errors:
        log(f"[CRITICAL] 處理器仍有驗證錯誤: {errors}")
    else:
        # 啟動處理器
        current_state = updated_proc.component.state
        if current_state == "STOPPED":
            log("-> 發送啟動指令 (schedule_processor scheduled=True)...")
            try:
                nipyapi.canvas.schedule_processor(updated_proc, scheduled=True)
                log("[SUCCESS] PutSQL 處理器已成功啟動！")
            except Exception as e:
                log(f"[WARN] 啟動調度提示: {e}")

    return updated_proc


def create_local_2_sql_pg(parent_pg=None, *args, **kwargs):
    """主進入點函式"""
    log("==========================================")
    log("=== [START] Local_2_SQL 流程圖配置部署 ===")
    log("==========================================")
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

        log(f"[OK] 找到目標 PG: {local_pg.component.name} ({local_pg.id})")

        db_config = kwargs.get("db_config", DEFAULT_DB_CONFIG)
        dbcp_properties = {
            "Database Connection URL": db_config["url"],
            "Database Driver Class Name": db_config["driver_class"],
            "database-driver-locations": db_config["driver_location"],
            "Database User": db_config["user"],
            "Password": db_config["password"],
        }

        # 1. 確保 DBCP 連線池服務啟動
        dbcp_svc = ensure_local_controller_service(
            local_pg=local_pg,
            service_type="org.apache.nifi.dbcp.DBCPConnectionPool",
            service_name="PostgreSQL_DBCP_Pool",
            properties=dbcp_properties,
        )

        # 2. 將寫入處理器轉為 PutSQL 並啟動
        table_name = kwargs.get("table_name", "raw_bike_availability")
        switch_to_putsql_processor(local_pg, dbcp_svc, table_name=table_name)

        log("==========================================")
        log("=== [FINISH] Local_2_SQL 部署全部順利完成 ===")
        log("==========================================")
        return local_pg

    except Exception as e:
        log(f"[FATAL ERROR] 部署過程發生致命例外: {e}")
        traceback.print_exc()
        raise e


if __name__ == "__main__":
    nipyapi.config.nifi_config.host = "http://127.0.0.1:8080/nifi-api"
    pg = create_local_2_sql_pg()
    try:
        nipyapi.canvas.schedule_process_group(pg.id, scheduled=True)
    except Exception as e:
        log(f"[INFO] schedule_process_group 提示: {e}")