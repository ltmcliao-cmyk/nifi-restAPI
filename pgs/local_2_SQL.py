#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import nipyapi

# 預設 NiFi 連線位置與元件常數
DEFAULT_NIFI_HOST = "http://127.0.0.1:8080/nifi-api"
DEFAULT_PG_ID = "10d7800a-01a1-1000-e2d4-9b50ad6cf816"
PUT_DB_PROCESSOR_ID = "10d78562-01a1-1000-adec-5b289ce54b88"

# 資料庫連線配置
DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/your_database",
    "driver_class": "org.postgresql.Driver",
    "driver_location": "/opt/nifi/drivers/postgresql-42.2.14.jar",
    "user": "postgres",
    "password": "your_password",
    "table_name": "target_table",
    "statement_type": "INSERT",
}


def init_nipyapi_config(nifi_url: str = DEFAULT_NIFI_HOST):
    """初始化 nipyapi 連線端點"""
    nipyapi.config.nifi_config.host = nifi_url


def get_or_enable_controller_service(
    cs_entity: nipyapi.nifi.ControllerServiceEntity,
) -> nipyapi.nifi.ControllerServiceEntity:
    """透過 nipyapi 啟用指定的 Controller Service"""
    cs_api = nipyapi.nifi.ControllerServicesApi()
    current_cs = cs_api.get_controller_service(cs_entity.id)

    if current_cs.component.state != "ENABLED":
        run_status = nipyapi.nifi.ControllerServiceRunStatusEntity(
            revision=current_cs.revision, state="ENABLED"
        )
        current_cs = cs_api.update_run_status(
            id=current_cs.id, body=run_status
        )
        print(f"[+] Controller Service 已啟用: {current_cs.component.name} ({current_cs.id})")
    return current_cs


def get_or_create_dbcp_service(
    pg_entity: nipyapi.nifi.ProcessGroupEntity,
) -> nipyapi.nifi.ControllerServiceEntity:
    """在指定 Process Group 內透過 nipyapi 建立或取得 DBCPConnectionPool"""
    flow_api = nipyapi.nifi.FlowApi()
    pg_api = nipyapi.nifi.ProcessGroupsApi()

    # 1. 檢查 Process Group 內是否已存在
    services_resp = flow_api.get_controller_services_from_group(pg_entity.id)
    if services_resp.controller_services:
        for cs in services_resp.controller_services:
            if "DBCPConnectionPool" in cs.component.type:
                return get_or_enable_controller_service(cs)

    # 2. 建立新 DBCPConnectionPool
    cs_dto = nipyapi.nifi.ControllerServiceDTO(
        name="PostgreSQL Connection Pool",
        type="org.apache.nifi.dbcp.DBCPConnectionPool",
        properties={
            "Database Connection URL": DB_CONFIG["url"],
            "Database Driver Class Name": DB_CONFIG["driver_class"],
            "Database Driver Location(s)": DB_CONFIG["driver_location"],
            "Database User": DB_CONFIG["user"],
            "Password": DB_CONFIG["password"],
        },
    )
    new_entity = nipyapi.nifi.ControllerServiceEntity(
        revision=nipyapi.nifi.RevisionDTO(version=0), component=cs_dto
    )
    created_cs = pg_api.create_controller_service(
        id=pg_entity.id, body=new_entity
    )
    return get_or_enable_controller_service(created_cs)


def get_or_create_record_reader(
    pg_entity: nipyapi.nifi.ProcessGroupEntity,
) -> nipyapi.nifi.ControllerServiceEntity:
    """在指定 Process Group 內透過 nipyapi 建立或取得 JsonTreeReader"""
    flow_api = nipyapi.nifi.FlowApi()
    pg_api = nipyapi.nifi.ProcessGroupsApi()

    # 1. 檢查是否已存在
    services_resp = flow_api.get_controller_services_from_group(pg_entity.id)
    if services_resp.controller_services:
        for cs in services_resp.controller_services:
            if "JsonTreeReader" in cs.component.type:
                return get_or_enable_controller_service(cs)

    # 2. 建立新 JsonTreeReader
    cs_dto = nipyapi.nifi.ControllerServiceDTO(
        name="Default JsonTreeReader",
        type="org.apache.nifi.json.JsonTreeReader",
    )
    new_entity = nipyapi.nifi.ControllerServiceEntity(
        revision=nipyapi.nifi.RevisionDTO(version=0), component=cs_dto
    )
    created_cs = pg_api.create_controller_service(
        id=pg_entity.id, body=new_entity
    )
    return get_or_enable_controller_service(created_cs)


def fix_and_configure_put_database_record(
    pg_entity: nipyapi.nifi.ProcessGroupEntity,
    dbcp_service: nipyapi.nifi.ControllerServiceEntity,
    reader_service: nipyapi.nifi.ControllerServiceEntity,
):
    """使用 nipyapi.canvas 更新 PutDatabaseRecord 處理器屬性"""
    # 尋找處理器
    target_proc = nipyapi.canvas.get_processor(
        PUT_DB_PROCESSOR_ID, identifier_type="id"
    )
    if not target_proc:
        target_proc = nipyapi.canvas.get_processor(
            "PutDatabaseRecord to PostgreSQL", identifier_type="name"
        )

    if not target_proc:
        raise ValueError("找不到處理器 'PutDatabaseRecord to PostgreSQL'")

    # 使用現有 config 物件進行局部屬性覆寫，保留預設調度與環境參數
    current_config = target_proc.component.config
    current_config.properties["Record Reader"] = reader_service.id
    current_config.properties["Database Connection Pooling Service"] = (
        dbcp_service.id
    )
    current_config.properties["Statement Type"] = DB_CONFIG["statement_type"]
    current_config.properties["Table Name"] = DB_CONFIG["table_name"]
    current_config.auto_terminated_relationships = ["success", "failure", "retry"]

    # 透過 nipyapi.canvas.update_processor 送出更新
    updated_proc = nipyapi.canvas.update_processor(target_proc, current_config)
    print(
        f"[+] 處理器 {updated_proc.component.name} 屬性已成功更新並修復驗證錯誤"
    )


def create_local_2_sql_pg(
    parent_pg=None,
    nifi_url: str = DEFAULT_NIFI_HOST,
    pg_id: str = DEFAULT_PG_ID,
    **kwargs,
) -> nipyapi.nifi.ProcessGroupEntity:
    """
    提供給 main.py 呼叫的入口函式。
    完全使用 nipyapi 第一層封裝取得/建立 Process Group 並完成服務配置，
    保證回傳 ProcessGroupEntity 物件。
    """
    init_nipyapi_config(nifi_url)

    # 1. 取得目標 Process Group（以 ID 或名稱優先搜尋）
    local_pg = nipyapi.canvas.get_process_group(pg_id, identifier_type="id")
    if not local_pg:
        local_pg = nipyapi.canvas.get_process_group(
            "Local_2_SQL", identifier_type="name"
        )

    # 若不存在則在 parent_pg 下建立
    if not local_pg:
        parent_entity = parent_pg
        if not parent_entity:
            root_id = nipyapi.canvas.get_root_pg_id()
            parent_entity = nipyapi.canvas.get_process_group(root_id, "id")
        elif isinstance(parent_entity, str):
            parent_entity = nipyapi.canvas.get_process_group(
                parent_entity, "id"
            )

        local_pg = nipyapi.canvas.create_process_group(
            parent_entity, "Local_2_SQL", location=(0, 0)
        )
        print(f"[+] 新建 Local_2_SQL Process Group: {local_pg.id}")

    # 2. 透過 nipyapi 建立並啟用相依的 Controller Services
    dbcp_service = get_or_create_dbcp_service(local_pg)
    reader_service = get_or_create_record_reader(local_pg)

    # 3. 透過 nipyapi.canvas 更新處理器配置
    fix_and_configure_put_database_record(
        local_pg, dbcp_service, reader_service
    )

    # 4. 回傳標準 nipyapi.nifi.ProcessGroupEntity 物件（具備 .id 屬性）
    return local_pg


if __name__ == "__main__":
    pg = create_local_2_sql_pg()
    # 測試本地排程啟動
    nipyapi.canvas.schedule_process_group(pg.id, scheduled=True)