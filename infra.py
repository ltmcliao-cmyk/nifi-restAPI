# -*- coding: utf-8 -*-
"""
infra.py - 不常改動的基礎設施配置模組
設計邏輯與精神：
1. 第一性原理：資料庫連線池 (DBCP) 與 Record Reader 為全域/PG 共享之基礎服務。
2. 操作第一層封裝：使用 nipyapi.canvas 提供的 create_controller, update_controller, schedule_controller。
3. 冪等性防護：若元件已存在且處於正確狀態，直接沿用，避免版本鎖衝突。
"""

import nipyapi


def init_dbcp_pool(parent_pg, db_config):
    """
    建立並確保 PostgreSQL DBCPConnectionPool Controller Service 可用。

    Args:
        parent_pg (ProcessGroupEntity): 掛載此服務的父級 Process Group。
        db_config (dict): 資料庫連線配置參數。

    Returns:
        ControllerServiceEntity: 已啟用的 DBCP Controller Service 實例。
    """
    pool_name = "PostgreSQL_DBCP_Pool"

    # 1. 檢查是否已存在同名 Controller Service
    existing_services = nipyapi.canvas.list_all_controllers(parent_pg.id)
    target_service = None
    if existing_services:
        for svc in existing_services:
            if svc.component.name == pool_name:
                target_service = svc
                break

    # 2. 不存在則建立並設定屬性
    if not target_service:
        types = nipyapi.canvas.get_controller_type('org.apache.nifi.dbcp.DBCPConnectionPool', identifier_type='name')
        if isinstance(types, list):
            dbcp_type = types[0] if types else nipyapi.canvas.get_controller_type('DBCPConnectionPool')[0]
        else:
            dbcp_type = types

        target_service = nipyapi.canvas.create_controller(
            parent_pg=parent_pg,
            controller=dbcp_type,
            name=pool_name
        )

        config_dto = nipyapi.nifi.ControllerServiceDTO(
            properties={
                'Database Connection URL': db_config.get('url', 'jdbc:postgresql://postgres:5432/pipeline_db'),
                'Database Driver Class Name': db_config.get('driver_class', 'org.postgresql.Driver'),
                'Database Driver Location(s)': db_config.get('driver_location', '/opt/nifi/nifi-current/drivers/postgresql-42.7.3.jar'),
                'Database User': db_config.get('user', 'postgres'),
                'Password': db_config.get('password', 'postgrespassword123')
            }
        )
        target_service = nipyapi.canvas.update_controller(target_service, config_dto)

    # 3. 確保服務處於啟用狀態（更新前先刷新最新版本號）
    target_service = nipyapi.canvas.get_controller(target_service.id, 'id')
    if target_service.component.state != 'ENABLED':
        nipyapi.canvas.schedule_controller(target_service, scheduled=True)
        target_service = nipyapi.canvas.get_controller(target_service.id, 'id')

    return target_service


def init_json_reader(parent_pg):
    """
    建立並啟用 JsonTreeReader Controller Service。

    Args:
        parent_pg (ProcessGroupEntity): 掛載此服務的 Process Group。

    Returns:
        ControllerServiceEntity: 已啟用的 JsonTreeReader Controller Service 實例。
    """
    service_name = "JsonTreeReader"

    # 1. 檢查是否已存在同名服務
    existing_services = nipyapi.canvas.list_all_controllers(parent_pg.id)
    target_service = None
    if existing_services:
        for svc in existing_services:
            if svc.component.name == service_name:
                target_service = svc
                break

    # 2. 不存在則建立
    if not target_service:
        types = nipyapi.canvas.get_controller_type('org.apache.nifi.json.JsonTreeReader', identifier_type='name')
        if isinstance(types, list):
            reader_type = types[0] if types else nipyapi.canvas.get_controller_type('JsonTreeReader')[0]
        else:
            reader_type = types

        target_service = nipyapi.canvas.create_controller(
            parent_pg=parent_pg,
            controller=reader_type,
            name=service_name
        )

    # 3. 重新獲取最新實例（避免 Revision 衝突引發 400 Bad Request）
    target_service = nipyapi.canvas.get_controller(target_service.id, 'id')

    # 4. 若尚未啟用則啟用
    if target_service.component.state != 'ENABLED':
        nipyapi.canvas.schedule_controller(target_service, scheduled=True)
        target_service = nipyapi.canvas.get_controller(target_service.id, 'id')

    return target_service


def quiesce_process_group(pg_id):
    """
    通用 Quiesce 函式：安全停止指定 Process Group 及其底下所有組件。
    """
    return nipyapi.canvas.schedule_process_group(process_group_id=pg_id, scheduled=False)