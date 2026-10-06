#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - Local_2_SQL 流程圖模組 (同層封閉保證生效版)
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
    """在指定 PG (同層) 建立、配置並啟動 Controller Service"""
    # 1. 取得該 PG 內的所有服務
    services = nipyapi.canvas.list_all_controllers(parent_pg.id)
    svc = next((s for s in services if s.component.name == service_name), None)

    # 2. 不存在則建立在該 PG 內部 (同層作用域)
    if not svc:
        print(f"[*] 在 PG [{parent_pg.component.name}] 建立 Controller: {service_name}...")
        svc = nipyapi.canvas.create_controller(
            parent_pg=parent_pg,
            controller_type=service_type,
            name=service_name
        )

    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    # 3. 配置屬性 (若為 ENABLED 需先暫停以套用修改)
    if properties:
        if svc.component.state == "ENABLED":
            nipyapi.canvas.schedule_controller(svc, scheduled=False)
            time.sleep(0.5)
            svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
            if isinstance(svc, list):
                svc = svc[0]

        svc.component.properties = properties
        svc = nipyapi.canvas.update_controller(svc, svc.component)

    # 4. 啟用 Controller Service (必須處於 ENABLED，Processor 才能綁定成功)
    svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
    if isinstance(svc, list):
        svc = svc[0]

    if svc.component.state != "ENABLED":
        print(f"[*] 啟用 Controller Service: {service_name}...")
        try:
            nipyapi.canvas.schedule_controller(svc, scheduled=True)
        except Exception as e:
            print(f"[!] schedule_controller 警告: {e}")

        # 等待啟用完成
        for _ in range(20):
            svc = nipyapi.canvas.get_controller(svc.id, identifier_type="id")
            if isinstance(svc, list):
                svc = svc[0]
            if svc.component.state == "ENABLED":
                print(f"[+] {service_name} 已成功轉為 ENABLED！")
                break
            time.sleep(0.5)

    return svc


def repair_and_run_put_database_record(local_pg, dbcp_svc, json_reader_svc, table_name="target_table"):
    """綁定 4 大屬性並立即切換為 RUNNING"""
    # 1. 取得目標 Processor
    proc = nipyapi.canvas.get_processor(PUT_DB_PROC_ID, identifier_type="id")
    if not proc:
        proc = nipyapi.canvas.get_processor(PUT_DB_PROC_NAME, identifier_type="name")
        if isinstance(proc, list) and proc:
            proc = proc[0]

    if not proc:
        raise ValueError(f"找不到處理器: {PUT_DB_PROC_NAME}")

    proc = nipyapi.canvas.get_processor(proc.id, identifier_type="id")
    if isinstance(proc, list):
        proc = proc[0]

    # 2. 寫入標準 Display Name 屬性
    config = proc.component.config
    config.properties["Record Reader"] = json_reader_svc.id
    config.properties["Database Connection Pooling Service"] = dbcp_svc.id
    config.properties["Statement Type"] = "INSERT"
    config.properties["Table Name"] = table_name

    valid_rels = [rel.name for rel in proc.component.relationships]
    config.auto_terminated_relationships = [
        r for r in ["success", "failure", "retry"] if r in valid_rels
    ]

    # 3. 提交更新
    print(f"[*] 正在套用屬性至 {PUT_DB_PROC_NAME}...")
    updated_proc = nipyapi.canvas.update_processor(proc, config)
    time.sleep(1)

    # 4. 重新抓取驗證狀態
    updated_proc = nipyapi.canvas.get_processor(updated_proc.id, identifier_type="id")
    if isinstance(updated_proc, list):
        updated_proc = updated_proc[0]

    print(f"[*] 驗證錯誤列表: {updated_proc.component.validation_errors}")

    # 5. 啟動處理器
    print(f"[*] 正在將處理器切換至 RUNNING...")
    nipyapi.canvas.schedule_processor(updated_proc, scheduled=True)
    print(f"[+] PutDatabaseRecord 已啟動！開始消化 Matched to SQL 佇列。")
    return updated_proc


def create_local_2_sql_pg(parent_pg=None, *args, **kwargs):
    """主進入點函式"""
    # 1. 取得 Local_2_SQL PG 實體
    local_pg = nipyapi.canvas.get_process_group(PROCESS_GROUP_ID, identifier_type="id")
    if not local_pg:
        local_pg = nipyapi.canvas.get_process_group(PROCESS_GROUP_NAME, identifier_type="name")
        if isinstance(local_pg, list) and local_pg:
            local_pg = local_pg[0]

    if not local_pg:
        raise ValueError(f"找不到 Process Group: {PROCESS_GROUP_NAME} ({PROCESS_GROUP_ID})")

    # 2. 直接在 local_pg 內部建立/獲取 Controller Services（同層作用域，解決跨層解析盲區）
    db_config = kwargs.get("db_config", DEFAULT_DB_CONFIG)
    dbcp_properties = {
        "Database Connection URL": db_config["url"],
        "Database Driver Class Name": db_config["driver_class"],
        "database-driver-locations": db_config["driver_location"],
        "Database User": db_config["user"],
        "Password": db_config["password"],
    }

    print("[*] 正在為 Local_2_SQL 建立同層 Controller Services...")
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

    # 3. 修復 Processor 並啟動
    table_name = kwargs.get("table_name", "raw_bike_availability")
    repair_and_run_put_database_record(
        local_pg, dbcp_svc, json_reader_svc, table_name=table_name
    )

    return local_pg


if __name__ == "__main__":
    nipyapi.config.nifi_config.host = "http://127.0.0.1:8080/nifi-api"
    pg = create_local_2_sql_pg()
    nipyapi.canvas.schedule_process_group(pg.id, scheduled=True)
    print(f"[+] Local_2_SQL 排程啟動完成。")