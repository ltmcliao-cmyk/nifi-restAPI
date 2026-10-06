#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - Local_2_SQL 流程圖模組 (完整自動修復穩定版)
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


def ensure_controller_service(parent_pg, service_type, service_name, properties=None):
    """在 local_pg 內部建立並啟用 Controller Service (同層作用域)"""
    services = nipyapi.canvas.list_all_controllers(parent_pg.id)
    svc = next((s for s in services if s.component.name == service_name), None)

    if not svc:
        print(f"[*] 建立 Controller Service: {service_name}...")
        svc = nipyapi.canvas.create_controller(
            parent_pg=parent_pg,
            controller_type=service_type,
            name=service_name
        )

    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    if properties:
        if svc.component.state == "ENABLED":
            nipyapi.canvas.schedule_controller(svc, scheduled=False)
            time.sleep(0.5)
            svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
            if isinstance(svc, list):
                svc = svc[0]

        svc.component.properties = properties
        svc = nipyapi.canvas.update_controller(svc, svc.component)

    # 確保 Controller Service 轉為 ENABLED
    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    if svc.component.state != "ENABLED":
        print(f"[*] 啟用 Controller Service: {service_name}...")
        try:
            nipyapi.canvas.schedule_controller(svc, scheduled=True)
        except Exception as e:
            print(f"[!] schedule_controller 提示: {e}")

        for _ in range(20):
            svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
            if isinstance(svc, list):
                svc = svc[0]
            if svc.component.state == "ENABLED":
                print(f"[+] {service_name} 已成功轉為 ENABLED！")
                break
            time.sleep(0.5)

    return svc


def repair_and_run_put_database_record(local_pg, dbcp_svc, json_reader_svc, table_name="raw_bike_availability"):
    """更新屬性，並安全地處理啟動狀態"""
    # 1. 取得目標 Processor
    proc = nipyapi.canvas.get_processor(PUT_DB_PROC_ID, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    if not proc:
        proc = nipyapi.canvas.get_processor(PUT_DB_PROC_NAME, identifier_type="name")
        if isinstance(proc, list) and proc:
            proc = proc[0]

    if not proc:
        raise ValueError(f"找不到處理器: {PUT_DB_PROC_NAME}")

    proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    # 2. 寫入 4 大必要屬性與路由關係
    config = proc.component.config
    config.properties["Record Reader"] = json_reader_svc.id
    config.properties["Database Connection Pooling Service"] = dbcp_svc.id
    config.properties["Statement Type"] = "INSERT"
    config.properties["Table Name"] = table_name

    valid_rels = [rel.name for rel in proc.component.relationships]
    config.auto_terminated_relationships = [
        r for r in ["success", "failure", "retry"] if r in valid_rels
    ]

    print(f"[*] 正在更新 PutDatabaseRecord 屬性配置...")
    updated_proc = nipyapi.canvas.update_processor(proc, config)
    time.sleep(1)

    # 3. 取得最新狀態以進行防禦性啟動
    updated_proc = nipyapi.canvas.get_processor(updated_proc.id, identifier_type="id")
    if isinstance(updated_proc, list):
        updated_proc = updated_proc[0]

    current_state = updated_proc.component.state
    print(f"[*] 處理器當前狀態: {current_state}, 驗證錯誤: {updated_proc.component.validation_errors}")

    # 4. 防禦性啟動：避免重複發送 start 指令導致 STARTING 異常
    if current_state == "STOPPED":
        print(f"[*] 正在啟動 PutDatabaseRecord 處理器...")
        try:
            nipyapi.canvas.schedule_processor(updated_proc, scheduled=True)
            print(f"[+] PutDatabaseRecord 啟動指令已送出！")
        except Exception as e:
            # 若已經處於 STARTING/RUNNING 則安全忽略
            if "cannot be started because it is not stopped" in str(e):
                print(f"[!] 處理器已在啟動中 (STARTING/RUNNING)，略過重複啟動。")
            else:
                raise e
    else:
        print(f"[+] 處理器當前為 {current_state}，無需重複啟動。")

    return updated_proc


def create_local_2_sql_pg(parent_pg=None, *args, **kwargs):
    """主進入點函式"""
    local_pg = nipyapi.canvas.get_process_group(PROCESS_GROUP_ID, identifier_type="id")
    if isinstance(local_pg, list):
        local_pg = local_pg[0]

    if not local_pg:
        local_pg = nipyapi.canvas.get_process_group(PROCESS_GROUP_NAME, identifier_type="name")
        if isinstance(local_pg, list) and local_pg:
            local_pg = local_pg[0]

    if not local_pg:
        raise ValueError(f"找不到 Process Group: {PROCESS_GROUP_NAME} ({PROCESS_GROUP_ID})")

    # 1. 確保同層 Controller Services 建立並 ENABLED
    db_config = kwargs.get("db_config", DEFAULT_DB_CONFIG)
    dbcp_properties = {
        "Database Connection URL": db_config["url"],
        "Database Driver Class Name": db_config["driver_class"],
        "database-driver-locations": db_config["driver_location"],
        "Database User": db_config["user"],
        "Password": db_config["password"],
    }

    dbcp_svc = ensure_controller_service(
        parent_pg=local_pg,
        service_type="org.apache.nifi.dbcp.DBCPConnectionPool",
        service_name="PostgreSQL_DBCP_Pool",
        properties=dbcp_properties,
    )

    json_reader_svc = ensure_controller_service(
        parent_pg=local_pg,
        service_type="org.apache.nifi.json.JsonTreeReader",
        service_name="JsonTreeReader_Local",
        properties={},
    )

    # 2. 修復 Processor 配置
    table_name = kwargs.get("table_name", "raw_bike_availability")
    repair_and_run_put_database_record(
        local_pg, dbcp_svc, json_reader_svc, table_name=table_name
    )

    return local_pg


if __name__ == "__main__":
    nipyapi.config.nifi_config.host = "http://127.0.0.1:8080/nifi-api"
    pg = create_local_2_sql_pg()
    # 防禦性排程整組 PG
    try:
        nipyapi.canvas.schedule_process_group(pg.id, scheduled=True)
    except Exception as e:
        print(f"[!] PG 排程提示: {e}")
    print(f"[+] Local_2_SQL 排程啟動完成。")