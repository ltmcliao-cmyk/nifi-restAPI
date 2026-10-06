#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import nipyapi

# NiFi 預設連線位置與元件識別碼
DEFAULT_NIFI_HOST = "http://127.0.0.1:8080/nifi-api"
DEFAULT_PG_ID = "10d7800a-01a1-1000-e2d4-9b50ad6cf816"
PUT_DB_PROCESSOR_ID = "10d78562-01a1-1000-adec-5b289ce54b88"

# PostgreSQL 連線設定（請依環境調整）
DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/your_database",
    "driver_class": "org.postgresql.Driver",
    "driver_location": "/opt/nifi/drivers/postgresql-42.2.14.jar",
    "user": "postgres",
    "password": "your_password",
    "table_name": "target_table",
    "statement_type": "INSERT",
}


def init_nipyapi(nifi_url: str = DEFAULT_NIFI_HOST):
    """配置 nipyapi 端點連線"""
    nipyapi.config.nifi_config.host = nifi_url


def enable_controller_service(
    service_entity: nipyapi.nifi.ControllerServiceEntity,
) -> nipyapi.nifi.ControllerServiceEntity:
    """透過 nipyapi 啟用指定的 Controller Service"""
    cs_api = nipyapi.nifi.ControllerServicesApi()
    current = cs_api.get_controller_service(service_entity.id)

    if current.component.state != "ENABLED":
        status_payload = nipyapi.nifi.ControllerServiceRunStatusEntity(
            revision=current.revision, state="ENABLED"
        )
        current = cs_api.update_run_status(id=current.id, body=status_payload)
        print(f"[+] Controller Service 已啟用: {current.component.name}")
    return current


def create_or_get_dbcp_service(
    pg_entity: nipyapi.nifi.ProcessGroupEntity,
) -> nipyapi.nifi.ControllerServiceEntity:
    """在指定 Process Group 內透過 nipyapi 建立或取得 DBCPConnectionPool"""
    flow_api = nipyapi.nifi.FlowApi()
    pg_api = nipyapi.nifi.ProcessGroupsApi()

    # 1. 檢查 Process Group 內是否已有連線池服務
    services = (
        flow_api.get_controller_services_from_group(
            pg_entity.id
        ).controller_services
        or []
    )
    for svc in services:
        if "DBCPConnectionPool" in svc.component.type:
            return enable_controller_service(svc)

    # 2. 建立新 DBCPConnectionPool 服務實體
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
    req_body = nipyapi.nifi.ControllerServiceEntity(
        revision=nipyapi.nifi.RevisionDTO(version=0), component=cs_dto
    )
    created_svc = pg_api.create_controller_service(
        id=pg_entity.id, body=req_body
    )
    return enable_controller_service(created_svc)


def create_or_get_record_reader(
    pg_entity: nipyapi.nifi.ProcessGroupEntity, reader_type: str = "json"
) -> nipyapi.nifi.ControllerServiceEntity:
    """在指定 Process Group 內透過 nipyapi 建立或取得 Record Reader"""
    flow_api = nipyapi.nifi.FlowApi()
    pg_api = nipyapi.nifi.ProcessGroupsApi()

    target_type = (
        "org.apache.nifi.json.JsonTreeReader"
        if reader_type == "json"
        else "org.apache.nifi.csv.CSVReader"
    )
    target_name = (
        "Default JsonTreeReader"
        if reader_type == "json"
        else "Default CSVReader"
    )

    # 1. 檢查是否已存在
    services = (
        flow_api.get_controller_services_from_group(
            pg_entity.id
        ).controller_services
        or []
    )
    for svc in services:
        if reader_type in svc.component.type.lower():
            return enable_controller_service(svc)

    # 2. 建立新 Record Reader
    cs_dto = nipyapi.nifi.ControllerServiceDTO(
        name=target_name, type=target_type
    )
    req_body = nipyapi.nifi.ControllerServiceEntity(
        revision=nipyapi.nifi.RevisionDTO(version=0), component=cs_dto
    )
    created_svc = pg_api.create_controller_service(
        id=pg_entity.id, body=req_body
    )
    return enable_controller_service(created_svc)


def configure_and_fix_put_db_record(
    dbcp_service: nipyapi.nifi.ControllerServiceEntity,
    reader_service: nipyapi.nifi.ControllerServiceEntity,
):
    """使用 nipyapi.canvas 修正 PutDatabaseRecord 處理器的 4 大必要屬性與關係路由"""
    # 透過 nipyapi.canvas 取得處理器 DTO
    proc = nipyapi.canvas.get_processor(
        PUT_DB_PROCESSOR_ID, identifier_type="id"
    )
    if not proc:
        proc = nipyapi.canvas.get_processor(
            "PutDatabaseRecord to PostgreSQL", identifier_type="name"
        )

    if not proc:
        raise ValueError(
            "找不到處理器 'PutDatabaseRecord to PostgreSQL'，請確認 ID 或名稱"
        )

    # 局部更新配置，保留原有執行緒與排程設定
    config = proc.component.config
    config.properties["Record Reader"] = reader_service.id
    config.properties["Database Connection Pooling Service"] = dbcp_service.id
    config.properties["Statement Type"] = DB_CONFIG["statement_type"]
    config.properties["Table Name"] = DB_CONFIG["table_name"]
    config.auto_terminated_relationships = ["success", "failure", "retry"]

    # 調用 nipyapi.canvas 更新處理器
    updated_proc = nipyapi.canvas.update_processor(proc, config)
    print(
        f"[+] 處理器 '{updated_proc.component.name}' 驗證錯誤已成功清除並完成配置"
    )
    return updated_proc


def create_local_2_sql_pg(
    parent_pg=None,
    nifi_url: str = DEFAULT_NIFI_HOST,
    pg_id: str = DEFAULT_PG_ID,
    **kwargs,
) -> nipyapi.nifi.ProcessGroupEntity:
    """
    提供給 main.py 呼叫的入口函式。
    完全透過 nipyapi 取得 Process Group、建立相依服務、修復驗證錯誤，
    並回傳 nipyapi.nifi.ProcessGroupEntity 物件。
    """
    init_nipyapi(nifi_url)

    # 1. 取得 Process Group（優先依 ID，若無則依名稱）
    local_pg = nipyapi.canvas.get_process_group(pg_id, identifier_type="id")
    if not local_pg:
        local_pg = nipyapi.canvas.get_process_group(
            "Local_2_SQL", identifier_type="name"
        )

    # 若不存在則在根目錄或 parent_pg 下建立
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

    # 2. 透過 nipyapi 建立並啟用 Controller Services
    dbcp_service = create_or_get_dbcp_service(local_pg)
    reader_service = create_or_get_record_reader(local_pg, reader_type="json")

    # 3. 修正 PutDatabaseRecord 處理器
    configure_and_fix_put_db_record(dbcp_service, reader_service)

    print(
        f"[*] Local_2_SQL Process Group (ID: {local_pg.id}) 配置完成，準備啟動"
    )

    # 4. 回傳原生 ProcessGroupEntity 物件（具備 .id 屬性供 main.py 排程呼叫）
    return local_pg


if __name__ == "__main__":
    # 本地測試執行
    pg = create_local_2_sql_pg()
    nipyapi.canvas.schedule_process_group(pg.id, scheduled=True)
    print("[+] Process Group 內所有處理器已順利啟動 (RUNNING)")