#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - Local_2_SQL 流程圖模組 (動態 Descriptor 鍵值對齊修復版)
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


def resolve_descriptor_key(descriptors, target_display_name, fallback_kebab):
    """
    從 NiFi 的 PropertyDescriptors 中動態提取該屬性在後端註冊的真實 Key
    """
    if not descriptors:
        return fallback_kebab

    # 1. 依據 displayName 比對
    for key, desc in descriptors.items():
        if desc.display_name and desc.display_name.strip().lower() == target_display_name.strip().lower():
            return key
        if desc.name and desc.name.strip().lower() == target_display_name.strip().lower():
            return key

    # 2. 依據 fallback kebab 比對
    for key, desc in descriptors.items():
        if key.strip().lower() == fallback_kebab.strip().lower():
            return key

    return fallback_kebab


def ensure_local_controller_service(local_pg, service_type, service_name, properties=None):
    """在 Local PG 建立並啟用 Controller Service，並輸出詳細狀態"""
    log(f"--- 檢查 Controller Service: {service_name} ---")
    services = nipyapi.canvas.list_all_controllers(local_pg.id)
    svc = next((s for s in services if s.component.name == service_name), None)

    if not svc:
        log(f"-> 服務不存在，正在於 PG [{local_pg.id}] 內部建立: {service_name} ({service_type})...")
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

    # 配置屬性
    if properties:
        if svc.component.state == "ENABLED":
            log(f"-> 暫停 {service_name} 以便覆寫屬性...")
            try:
                nipyapi.canvas.schedule_controller(svc, scheduled=False)
            except Exception as e:
                log(f"   [WARN] 暫停服務提示: {e}")
            time.sleep(1)
            svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
            if isinstance(svc, list):
                svc = svc[0]

        log(f"-> 正在更新 {service_name} 屬性...")
        svc.component.properties = properties
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


def repair_and_run_put_database_record(local_pg, dbcp_svc, json_reader_svc, table_name="raw_bike_availability"):
    """綁定屬性、印出驗證狀態，並安全啟動"""
    log("--- 開始修復 PutDatabaseRecord 處理器 ---")
    proc = nipyapi.canvas.get_processor(PUT_DB_PROC_ID, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    if not proc:
        proc = nipyapi.canvas.get_processor(PUT_DB_PROC_NAME, identifier_type="name")
        if isinstance(proc, list) and proc:
            proc = proc[0]

    if not proc:
        raise ValueError(f"找不到目標處理器: {PUT_DB_PROC_NAME} ({PUT_DB_PROC_ID})")

    proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    log(f"-> 目標處理器 UUID: {proc.id}")
    descriptors = proc.component.config.descriptors or {}

    # 1. 動態解析出 NiFi 1.12.1 接受的 Property Keys
    key_reader = resolve_descriptor_key(descriptors, "Record Reader", "record-reader")
    key_dbcp = resolve_descriptor_key(descriptors, "Database Connection Pooling Service", "dbcp-service")
    key_stmt = resolve_descriptor_key(descriptors, "Statement Type", "statement-type")
    key_table = resolve_descriptor_key(descriptors, "Table Name", "table-name")

    log(f"-> 解析得到實際 Key: Reader='{key_reader}', DBCP='{key_dbcp}', Statement='{key_stmt}', Table='{key_table}'")

    # 2. 同時寫入解析出的 Key 與 Display Name 達成雙保險
    props = proc.component.config.properties or {}
    
    # 填入解析 Key
    props[key_reader] = json_reader_svc.id
    props[key_dbcp] = dbcp_svc.id
    props[key_stmt] = "INSERT"
    props[key_table] = table_name

    # 填入標準 Display Name
    props["Record Reader"] = json_reader_svc.id
    props["Database Connection Pooling Service"] = dbcp_svc.id
    props["Statement Type"] = "INSERT"
    props["Table Name"] = table_name

    # 填入 Kebab-case
    props["record-reader"] = json_reader_svc.id
    props["dbcp-service"] = dbcp_svc.id
    props["statement-type"] = "INSERT"
    props["table-name"] = table_name

    proc.component.config.properties = props

    valid_rels = [rel.name for rel in proc.component.relationships]
    proc.component.config.auto_terminated_relationships = [
        r for r in ["success", "failure", "retry"] if r in valid_rels
    ]

    log("-> 提交更新至 NiFi REST API...")
    try:
        updated_proc = nipyapi.canvas.update_processor(proc, proc.component.config)
    except Exception as e:
        log(f"[WARN] canvas.update_processor 提示: {e}，改用底層 ProcessorsApi 更新...")
        proc_fresh = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
        proc_fresh.component.config.properties = props
        proc_fresh.component.config.auto_terminated_relationships = proc.component.config.auto_terminated_relationships
        updated_proc = nipyapi.nifi.ProcessorsApi().update_processor(id=proc.id, body=proc_fresh)

    # 3. 輪詢校驗狀態
    log("-> 等待 NiFi 完成底層校驗 (消除 4 大必填錯誤)...")
    for attempt in range(1, 16):
        time.sleep(0.5)
        updated_proc = nipyapi.canvas.get_processor(updated_proc.id, identifier_type="id")
        if isinstance(updated_proc, list):
            updated_proc = updated_proc[0]
        
        errors = updated_proc.component.validation_errors or []
        log(f"   [校驗檢查 {attempt}/15] 錯誤數量: {len(errors)}")
        if not errors:
            log("[SUCCESS] 處理器所有校驗錯誤已全部清除！")
            break

    errors = updated_proc.component.validation_errors or []
    if errors:
        log(f"[CRITICAL] 處理器仍有驗證錯誤殘留: {errors}")
        for err in errors:
            log(f"   * {err}")
    else:
        log("[SUCCESS] 4 項必填欄位驗證全部通過！")

    # 4. 啟動處理器
    current_state = updated_proc.component.state
    log(f"-> 處理器更新後狀態: {current_state}")

    if current_state == "STOPPED" and not errors:
        log("-> 發送啟動指令 (schedule_processor scheduled=True)...")
        try:
            nipyapi.canvas.schedule_processor(updated_proc, scheduled=True)
            log("[SUCCESS] 處理器已下達啟動命令！")
        except Exception as e:
            err_msg = str(e)
            if any(k in err_msg for k in ["cannot be started because it is not stopped", "STARTING", "RUNNING"]):
                log(f"[INFO] 處理器已處於過渡狀態 ({err_msg})，略過重複啟動。")
            else:
                log(f"[ERR] 啟動拋出異常: {e}")
                raise e
    else:
        log(f"[INFO] 處理器狀態為 {current_state} (或有未解錯誤)，略過直接啟動。")

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

        # 確保同層 Services 存在且 ENABLED
        dbcp_svc = ensure_local_controller_service(
            local_pg=local_pg,
            service_type="org.apache.nifi.dbcp.DBCPConnectionPool",
            service_name="PostgreSQL_DBCP_Pool",
            properties=dbcp_properties,
        )

        json_reader_svc = ensure_local_controller_service(
            local_pg=local_pg,
            service_type="org.apache.nifi.json.JsonTreeReader",
            service_name="JsonTreeReader_Local",
            properties={},
        )

        # 修復處理器並啟動
        table_name = kwargs.get("table_name", "raw_bike_availability")
        repair_and_run_put_database_record(
            local_pg, dbcp_svc, json_reader_svc, table_name=table_name
        )

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