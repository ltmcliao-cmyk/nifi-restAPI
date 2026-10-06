# -*- coding: utf-8 -*-
"""
infra.py - 不常改動的基礎設施配置模組
設計邏輯與精神：
1. 第一性原理：資料庫連線池 (DBCP) 為全域共享之基礎服務，獨立於特定業務邏輯之外。
2. 操作第一層封裝：使用 nipyapi.canvas 提供的 create_controller, update_controller, schedule_controller。
3. 避免全域變數：連線參數以傳入字典 (db_config) 管理，發揮正交性。
"""

import nipyapi

def init_dbcp_pool(parent_pg, db_config):
    """
    建立並啟用 PostgreSQL DBCPConnectionPool Controller Service。
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

    # 2. 不存在則建立
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

    # 3. 確保服務處於停用狀態 (若正在運行則無法修改 properties)
    target_service = nipyapi.canvas.get_controller(target_service.id, 'id')
    if target_service.component.state != 'DISABLED':
        nipyapi.canvas.schedule_controller(target_service, scheduled=False)
        target_service = nipyapi.canvas.get_controller(target_service.id, 'id')

    # 4. 取得最新 Revision 後再更新設定
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

    # 5. 重新啟用 Controller Service
    nipyapi.canvas.schedule_controller(target_service, scheduled=True)
    return target_service


def quiesce_process_group(pg_id):
    """
    通用 Quiesce 函式：安全停止指定 Process Group 及其底下所有組件。
    奧卡姆剃刀原則：不封裝複雜類別，單一函式直達目的。
    """
    return nipyapi.canvas.schedule_process_group(process_group_id=pg_id, scheduled=False)