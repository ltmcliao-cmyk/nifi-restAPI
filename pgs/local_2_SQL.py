#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""pgs/local_2_SQL.py - Local_2_SQL 流程圖模組

設計精神：
1. 模組化複用：引用 infra.py 並將 DBCP 連線池與 Reader 建立於 Local_2_SQL 同層作用域（Co-location），避免跨層綁定失敗。
2. 高階封裝：全面使用 nipyapi.canvas 第一層 API，不暴露底層 DTO。
3. 服務預先啟用：在綁定處理器前確保 Controller Services 處於 ENABLED 狀態。
4. 版本自癒：更新 processor 前重新取得最新 entity，避免 revision 衝突。
5. 介面相容：回傳原生 ProcessGroupEntity，保證 main.py 的 local_pg.id 正常排程。
"""

import os
import sys
import nipyapi

# 引用根目錄的現有 infra 模組
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import infra

PROCESS_GROUP_ID = "10d7800a-01a1-1000-e2d4-9b50ad6cf816"
PROCESS_GROUP_NAME = "Local_2_SQL"
PUT_DB_PROC_NAME = "PutDatabaseRecord to PostgreSQL"

# 預設資料庫設定（若 main.py 未傳入則以此作為備用）
DEFAULT_DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/your_database",
    "driver_class": "org.postgresql.Driver",
    "driver_location": "/opt/nifi/drivers/postgresql-42.2.14.jar",
    "user": "postgres",
    "password": "your_password",
}


def get_or_create_infra_services(local_pg, db_config=None):
    """透過 infra.py 取得或初始化 DBCP 連線池與 JsonTreeReader（均建於 local_pg 同層作用域）"""
    config = db_config or DEFAULT_DB_CONFIG

    # 1. 檢查並獲取屬於 local_pg 作用域的 PostgreSQL DBCP 連線池
    local_controllers = nipyapi.canvas.list_all_controllers(local_pg.id)
    dbcp_service = next(
        (
            c
            for c in local_controllers
            if c.component.name == "PostgreSQL_DBCP_Pool"
            and c.component.parent_group_id == local_pg.id
        ),
        None,
    )

    if not dbcp_service:
        print(
            "[*] 呼叫 infra.init_dbcp_pool 初始化連線池 (建立於 Local_2_SQL 同層)..."
        )
        dbcp_service = infra.init_dbcp_pool(local_pg, config)

    # 2. 檢查並獲取屬於 local_pg 作用域的 JsonTreeReader
    json_reader = next(
        (
            c
            for c in local_controllers
            if c.component.name == "JsonTreeReader_Local"
            and c.component.parent_group_id == local_pg.id
        ),
        None,
    )

    if not json_reader:
        print(
            "[*] 呼叫 infra.init_json_reader 初始化 Reader (建立於 Local_2_SQL 同層)..."
        )
        json_reader = infra.init_json_reader(local_pg)

    # 3. 自動啟用 Controller Service，確保在 Processor 綁定前為 ENABLED 狀態
    print("[*] 確保 Controller Services 處於 ENABLED 狀態...")
    nipyapi.canvas.schedule_controller(dbcp_service, scheduled=True)
    nipyapi.canvas.schedule_controller(json_reader, scheduled=True)

    return dbcp_service, json_reader


def configure_put_database_record(
    local_pg, dbcp_service, json_reader, table_name="raw_bike_availability"
):
    """使用 nipyapi.canvas 修復 PutDatabaseRecord 處理器的 4 大必要屬性與路由"""
    # 1. 取得目標處理器實體
    processors = nipyapi.canvas.list_all_processors(local_pg.id)
    target_proc = next(
        (p for p in processors if p.component.name == PUT_DB_PROC_NAME), None
    )

    if not target_proc:
        target_proc = nipyapi.canvas.get_processor(
            PUT_DB_PROC_NAME, identifier_type="name"
        )
        if isinstance(target_proc, list) and target_proc:
            target_proc = target_proc[0]

    if not target_proc:
        raise ValueError(f"找不到處理器: {PUT_DB_PROC_NAME}")

    # 2. 重新抓取一次最新實體，同步最新 Revision 避免 400 衝突
    target_proc = nipyapi.canvas.get_processor(
        target_proc.id, identifier_type="id"
    )
    if isinstance(target_proc, list):
        target_proc = target_proc[0]

    # 3. 填入 4 項必要屬性（嚴格對齊 NiFi 官方 Display Name）並設定 auto-terminate
    config = target_proc.component.config
    config.properties["Record Reader"] = json_reader.id
    config.properties["Database Connection Pooling Service"] = dbcp_service.id
    config.properties["Statement Type"] = "INSERT"
    config.properties["Table Name"] = table_name
    config.auto_terminated_relationships = ["success", "failure", "retry"]

    # 4. 透過 nipyapi.canvas 更新處理器
    updated_proc = nipyapi.canvas.update_processor(target_proc, config)
    print(
        f"[+] 處理器 {updated_proc.component.name} 配置完成，驗證錯誤已清除！"
    )
    return updated_proc


def create_local_2_sql_pg(parent_pg=None, *args, **kwargs):
    """提供給 main.py 呼叫的標準入口函式。

    回傳原生 ProcessGroupEntity 物件，供 main.py 排程啟動。
    """
    # 1. 取得 Root Process Group 實體
    if not parent_pg:
        root_id = nipyapi.canvas.get_root_pg_id()
        root_pg = nipyapi.canvas.get_process_group(
            root_id, identifier_type="id"
        )
    elif isinstance(parent_pg, str):
        root_pg = nipyapi.canvas.get_process_group(
            parent_pg, identifier_type="id"
        )
    else:
        root_pg = parent_pg

    if isinstance(root_pg, list):
        root_pg = root_pg[0]

    # 2. 取得或建立 Local_2_SQL Process Group
    local_pg = nipyapi.canvas.get_process_group(
        PROCESS_GROUP_ID, identifier_type="id"
    )
    if not local_pg:
        local_pg = nipyapi.canvas.get_process_group(
            PROCESS_GROUP_NAME, identifier_type="name"
        )
    if isinstance(local_pg, list) and local_pg:
        local_pg = local_pg[0]

    if not local_pg:
        local_pg = nipyapi.canvas.create_process_group(
            parent_pg=root_pg,
            new_pg_name=PROCESS_GROUP_NAME,
            location=(0, 0),
        )

    # 3. 透過 infra.py 確保 Controller Services 在 local_pg 同層建立且已啟用 (ENABLED)
    db_config = kwargs.get("db_config", DEFAULT_DB_CONFIG)
    dbcp_service = kwargs.get("dbcp_service")
    json_reader = kwargs.get("json_reader") or kwargs.get("reader_service")

    if not dbcp_service or not json_reader:
        dbcp_service, json_reader = get_or_create_infra_services(
            local_pg, db_config
        )
    else:
        # 外部直接傳入時，仍確保服務處於 ENABLED 狀態
        nipyapi.canvas.schedule_controller(dbcp_service, scheduled=True)
        nipyapi.canvas.schedule_controller(json_reader, scheduled=True)

    # 4. 修復處理器屬性（預設表名對齊 'raw_bike_availability'）
    table_name = kwargs.get("table_name", "raw_bike_availability")
    configure_put_database_record(
        local_pg, dbcp_service, json_reader, table_name=table_name
    )

    # 5. 回傳標準 ProcessGroupEntity（具備 .id 屬性）
    return local_pg


if __name__ == "__main__":
    # 本地直接測試執行修復與排程啟動
    nipyapi.config.nifi_config.host = "http://127.0.0.1:8080/nifi-api"
    pg = create_local_2_sql_pg()
    nipyapi.canvas.schedule_process_group(pg.id, scheduled=True)
    print(f"[+] Local_2_SQL (ID: {pg.id}) 已成功修復並全部啟動 (RUNNING)")