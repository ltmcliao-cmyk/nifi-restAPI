#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - Local_2_SQL 流程圖模組 (同層封閉與全防禦版)
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


def ensure_local_controller_service(local_pg, service_type, service_name, properties=None):
    """保證 Controller Service 建立在 Local_2_SQL 內部同層作用域，並處於 ENABLED 狀態"""
    # 1. 查找 local_pg 內部現存服務
    services = nipyapi.canvas.list_all_controllers(local_pg.id)
    svc = next((s for s in services if s.component.name == service_name), None)

    # 2. 若不存在，直接建在 local_pg 內 (消除跨層作用域盲區)
    if not svc:
        print(f"[*] 於 [{local_pg.component.name}] 內部建立 Controller: {service_name}...")
        svc = nipyapi.canvas.create_controller(
            parent_pg=local_pg,
            controller_type=service_type,
            name=service_name
        )

    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    # 3. 配置屬性 (若已啟用則先停用以套用設定)
    if properties:
        if svc.component.state == "ENABLED":
            print(f"[*] 暫停 {service_name} 以更新屬性...")
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

    # 4. 強制啟用 (ENABLE) 並輪詢等待生效
    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    if svc.component.state != "ENABLED":
        print(f"[*] 啟用 Controller Service: {service_name}...")
        try:
            nipyapi.canvas.schedule_controller(svc, scheduled=True)
        except Exception as e:
            print(f"[!] 啟用命令提示: {e}")

        for _ in range(20):
            time.sleep(0.5)
            svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
            if isinstance(svc, list):
                svc = svc[0]
            if svc.component.state == "ENABLED":
                print(f"[+] {service_name} 已切換為 ENABLED (綠色閃電)！")
                break

    return svc


def repair_and_run_put_database_record(local_pg, dbcp_svc, json_reader_svc, table_name="raw_bike_availability"):
    """精準綁定 4 大屬性並安全啟動處理器"""
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

    # 2. 同步最新實體與 revision
    proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    # 3. 精準寫入標準 Display Name 屬性
    config = proc.component.config
    config.properties["Record Reader"] = json_reader_svc.id
    config.properties["Database Connection Pooling Service"] = dbcp_svc.id
    config.properties["Statement Type"] = "INSERT"
    config.properties["Table Name"] = table_name

    valid_rels = [rel.name for rel in proc.component.relationships]
    config.auto_terminated_relationships = [
        r for r in ["success", "failure", "retry"] if r in valid_rels
    ]

    # 4. 套用更新
    print(f"[*] 正在寫入 4 大屬性至 {PUT_DB_PROC_NAME}...")
    updated_proc = nipyapi.canvas.update_processor(proc, config)

    # 5. 輪詢校驗狀態 (等待消除 validation_errors)
    print(f"[*] 等待 NiFi 完成校驗...")
    for _ in range(15):
        time.sleep(0.5)
        updated_proc = nipyapi.canvas.get_processor(updated_proc.id, identifier_type="id")
        if isinstance(updated_proc, list):
            updated_proc = updated_proc[0]
        if not updated_proc.component.validation_errors:
            break

    print(f"[*] 當前校驗錯誤: {updated_proc.component.validation_errors}")

    # 6. 防禦性啟動：攔截 STARTING / is not stopped 等並發例外
    current_state = updated_proc.component.state
    if current_state == "STOPPED":
        print(f"[*] 啟動 PutDatabaseRecord 處理器...")
        try:
            nipyapi.canvas.schedule_processor(updated_proc, scheduled=True)
            print(f"[+] 啟動指令已成功發送！")
        except Exception as e:
            err_msg = str(e)
            if any(k in err_msg for k in ["cannot be started because it is not stopped", "STARTING", "RUNNING"]):
                print(f"[+] 處理器正在切換狀態 ({err_msg})，略過重複排程。")
            else:
                raise e
    else:
        print(f"[+] 處理器當前狀態已為 {current_state}，無需重複啟動。")

    return updated_proc


def create_local_2_sql_pg(parent_pg=None, *args, **kwargs):
    """main.py 呼叫之標準入口函式"""
    # 1. 取得 Local_2_SQL PG
    local_pg = nipyapi.canvas.get_process_group(PROCESS_GROUP_ID, identifier_type="id")
    if isinstance(local_pg, list):
        local_pg = local_pg[0]

    if not local_pg:
        local_pg = nipyapi.canvas.get_process_group(PROCESS_GROUP_NAME, identifier_type="name")
        if isinstance(local_pg, list) and local_pg:
            local_pg = local_pg[0]

    if not local_pg:
        raise ValueError(f"找不到 Process Group: {PROCESS_GROUP_NAME} ({PROCESS_GROUP_ID})")

    # 2. 獲取資料庫連線參數
    db_config = kwargs.get("db_config", DEFAULT_DB_CONFIG)
    dbcp_properties = {
        "Database Connection URL": db_config["url"],
        "Database Driver Class Name": db_config["driver_class"],
        "database-driver-locations": db_config["driver_location"],
        "Database User": db_config["user"],
        "Password": db_config["password"],
    }

    # 3. 關鍵：直接在 local_pg 內部建立並啟用 DBCP 與 Reader
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

    # 4. 修復處理器並啟動
    table_name = kwargs.get("table_name", "raw_bike_availability")
    repair_and_run_put_database_record(
        local_pg, dbcp_svc, json_reader_svc, table_name=table_name
    )

    # 5. 回傳原生 ProcessGroupEntity 保證 main.py 相容性
    return local_pg


if __name__ == "__main__":
    nipyapi.config.nifi_config.host = "http://127.0.0.1:8080/nifi-api"
    pg = create_local_2_sql_pg()
    try:
        nipyapi.canvas.schedule_process_group(pg.id, scheduled=True)
    except Exception as e:
        print(f"[!] PG 排程防禦攔截: {e}")
    print(f"[+] Local_2_SQL 全部修復並啟動完成。")