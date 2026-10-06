#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - Local_2_SQL 流程圖模組 (全方位防禦健全版)
"""

import os
import sys
import time
import nipyapi

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


def safe_enable_controller_service(service_entity, timeout=20):
    """防禦性啟用 Controller Service，確保其完全達到 ENABLED 且無校驗錯誤"""
    if not service_entity:
        return None

    svc = nipyapi.canvas.get_controller(service_entity.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    # 若已啟用則直接回傳
    if svc.component.state == "ENABLED":
        return svc

    print(f"[*] 嘗試啟用 Controller Service: {svc.component.name} ({svc.id})...")
    try:
        nipyapi.canvas.schedule_controller(svc, scheduled=True)
    except Exception as e:
        err_msg = str(e)
        if "already" in err_msg or "ENABLED" in err_msg:
            pass
        else:
            print(f"[!] schedule_controller 警告: {err_msg}")

    # 輪詢確認狀態
    start_time = time.time()
    while time.time() - start_time < timeout:
        svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
        if isinstance(svc, list):
            svc = svc[0]

        if svc.component.state == "ENABLED":
            print(f"[+] {svc.component.name} 已成功切換為 ENABLED！")
            return svc

        time.sleep(0.5)

    print(f"[!] 警告: {svc.component.name} 於 {timeout} 秒內未達到 ENABLED 狀態，目前狀態: {svc.component.state}")
    return svc


def get_or_create_resilient_controller(target_pg, root_pg, service_type, service_name, properties=None):
    """跨層感知與自動同層修復 Controller Service"""
    # 1. 優先查找同層 (local_pg)
    services_local = nipyapi.canvas.list_all_controllers(target_pg.id)
    svc = next((s for s in services_local if s.component.name == service_name), None)

    # 2. 次之查找根層 (root_pg)
    if not svc and root_pg:
        services_root = nipyapi.canvas.list_all_controllers(root_pg.id)
        svc = next((s for s in services_root if s.component.name == service_name), None)

    # 3. 都不存在則直接在 local_pg 內部建立 (確保同層封閉作用域)
    if not svc:
        print(f"[*] 在 PG [{target_pg.component.name}] 建立 Controller Service: {service_name}...")
        svc = nipyapi.canvas.create_controller(
            parent_pg=target_pg,
            controller_type=service_type,
            name=service_name
        )

    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    # 4. 配置屬性防禦（若已啟用須先停用）
    if properties:
        if svc.component.state == "ENABLED":
            print(f"[*] 暫停 {service_name} 以便覆寫屬性...")
            try:
                nipyapi.canvas.schedule_controller(svc, scheduled=False)
            except Exception:
                pass
            time.sleep(1)
            svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
            if isinstance(svc, list):
                svc = svc[0]

        svc.component.properties = properties
        svc = nipyapi.canvas.update_controller(svc, svc.component)

    # 5. 強制切換為 ENABLED
    svc = safe_enable_controller_service(svc)
    return svc


def repair_and_run_put_database_record(local_pg, dbcp_svc, json_reader_svc, table_name="raw_bike_availability"):
    """安全修復 PutDatabaseRecord 的 4 大必要屬性與狀態調度"""
    # 1. 取得目標 Processor
    proc = nipyapi.canvas.get_processor(PUT_DB_PROC_ID, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    if not proc:
        proc = nipyapi.canvas.get_processor(PUT_DB_PROC_NAME, identifier_type="name")
        if isinstance(proc, list) and proc:
            proc = proc[0]

    if not proc:
        raise ValueError(f"找不到目標處理器: {PUT_DB_PROC_NAME} ({PUT_DB_PROC_ID})")

    # 2. 重新同步最新 revision 實體
    proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    # 3. 屬性注入（使用標準 Display Name）
    config = proc.component.config
    config.properties["Record Reader"] = json_reader_svc.id
    config.properties["Database Connection Pooling Service"] = dbcp_svc.id
    config.properties["Statement Type"] = "INSERT"
    config.properties["Table Name"] = table_name

    # 4. 安全配置 auto-terminate
    valid_rel_names = [rel.name for rel in proc.component.relationships]
    desired_terms = ["success", "failure", "retry"]
    config.auto_terminated_relationships = [r for r in desired_terms if r in valid_rel_names]

    # 5. 套用配置
    print(f"[*] 正在套用配置至 {PUT_DB_PROC_NAME}...")
    updated_proc = nipyapi.canvas.update_processor(proc, config)

    # 6. 防禦性輪詢驗證狀態（等待 NiFi 後台消除 validation_errors）
    print(f"[*] 等待處理器校驗與生效...")
    for _ in range(15):
        time.sleep(0.5)
        updated_proc = nipyapi.canvas.get_processor(updated_proc.id, identifier_type="id")
        if isinstance(updated_proc, list):
            updated_proc = updated_proc[0]
        # 當無驗證錯誤且處於穩定 STOPPED 狀態時跳出
        if not updated_proc.component.validation_errors:
            break

    errors = updated_proc.component.validation_errors or []
    if errors:
        print(f"[!] 警告: 處理器仍存在校驗錯誤: {errors}")
    else:
        print(f"[+] 處理器校驗完全通過，4 項必填項已全數滿足！")

    # 7. 防禦性啟動調度：防止並發競態與非 STOPPED 異常
    current_state = updated_proc.component.state
    if current_state == "STOPPED":
        print(f"[*] 發送啟動指令至 {PUT_DB_PROC_NAME}...")
        try:
            nipyapi.canvas.schedule_processor(updated_proc, scheduled=True)
            print(f"[+] 處理器啟動命令已確認發送！")
        except Exception as e:
            err_msg = str(e)
            if any(k in err_msg for k in ["cannot be started because it is not stopped", "STARTING", "RUNNING"]):
                print(f"[+] 處理器正在切換狀態 ({err_msg})，略過重複排程。")
            else:
                raise e
    else:
        print(f"[+] 處理器當前狀態已為 {current_state}，無需重複觸發啟動。")

    return updated_proc


def create_local_2_sql_pg(parent_pg=None, *args, **kwargs):
    """主進入點函式"""
    # 1. 取得 Root PG
    root_id = parent_pg if isinstance(parent_pg, str) else (parent_pg.id if parent_pg else None)
    if not root_id:
        root_id = nipyapi.canvas.get_root_pg_id()
    root_pg = nipyapi.canvas.get_process_group(root_id, identifier_type="id")
    if isinstance(root_pg, list):
        root_pg = root_pg[0]

    # 2. 取得 Local_2_SQL PG
    local_pg = nipyapi.canvas.get_process_group(PROCESS_GROUP_ID, identifier_type="id")
    if isinstance(local_pg, list):
        local_pg = local_pg[0]

    if not local_pg:
        local_pg = nipyapi.canvas.get_process_group(PROCESS_GROUP_NAME, identifier_type="name")
        if isinstance(local_pg, list) and local_pg:
            local_pg = local_pg[0]

    if not local_pg:
        raise ValueError(f"找不到 Process Group: {PROCESS_GROUP_NAME} ({PROCESS_GROUP_ID})")

    # 3. 建立並啟用 Controller Services
    db_config = kwargs.get("db_config", DEFAULT_DB_CONFIG)
    dbcp_properties = {
        "Database Connection URL": db_config["url"],
        "Database Driver Class Name": db_config["driver_class"],
        "database-driver-locations": db_config["driver_location"],
        "Database User": db_config["user"],
        "Password": db_config["password"],
    }

    # 優先同層建立與管理 DBCP Pool
    dbcp_svc = get_or_create_resilient_controller(
        target_pg=local_pg,
        root_pg=root_pg,
        service_type="org.apache.nifi.dbcp.DBCPConnectionPool",
        service_name="PostgreSQL_DBCP_Pool",
        properties=dbcp_properties,
    )

    # 查找或同層建立 JsonTreeReader
    json_reader_svc = get_or_create_resilient_controller(
        target_pg=local_pg,
        root_pg=root_pg,
        service_type="org.apache.nifi.json.JsonTreeReader",
        service_name="JsonTreeReader_Local",
        properties={},
    )

    # 4. 修復處理器並啟動
    table_name = kwargs.get("table_name", "raw_bike_availability")
    repair_and_run_put_database_record(
        local_pg, dbcp_svc, json_reader_svc, table_name=table_name
    )

    return local_pg


if __name__ == "__main__":
    nipyapi.config.nifi_config.host = "http://127.0.0.1:8080/nifi-api"
    pg = create_local_2_sql_pg()
    try:
        nipyapi.canvas.schedule_process_group(pg.id, scheduled=True)
    except Exception as e:
        print(f"[!] PG 排程防禦攔截: {e}")
    print(f"[+] Local_2_SQL 全部流程修復與排程執行完成。")