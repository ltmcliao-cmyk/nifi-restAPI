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

    Args:
        parent_pg (ProcessGroupEntity): 掛載此服務的父級 Process Group。
        db_config (dict): 資料庫連線配置參數。

    Returns:
        ControllerServiceEntity: 已啟用的 DBCP Controller Service 實例。
    """
    # 1. 取得 DBCPConnectionPool 的抽象型別定義 (第一層封裝: get_controller_type)
    dbcp_type = nipyapi.canvas.get_controller_type('DBCPConnectionPool')
    
    # 2. 於指定 PG 建立 Controller Service
    dbcp_service = nipyapi.canvas.create_controller(
        parent_pg=parent_pg,
        controller=dbcp_type,
        name="PostgreSQL_DBCP_Pool"
    )
    
    # 3. 填入連線屬性 (參考 compose.md 與 PostgreSQL 14 容器設定)
    config_dto = nipyapi.nifi.ControllerServiceDTO(
        properties={
            'Database Connection URL': db_config.get('url', 'jdbc:postgresql://postgres:5432/pipeline_db'),
            'Database Driver Class Name': db_config.get('driver_class', 'org.postgresql.Driver'),
            'Database Driver Location(s)': db_config.get('driver_location', '/opt/nifi/nifi-current/drivers/postgresql-42.7.3.jar'),
            'Database User': db_config.get('user', 'postgres'),
            'Password': db_config.get('password', 'postgrespassword123')
        }
    )
    nipyapi.canvas.update_controller(dbcp_service, config_dto)
    
    # 4. 啟用 Controller Service 供後續 Processor 使用
    nipyapi.canvas.schedule_controller(dbcp_service, scheduled=True)
    return dbcp_service


def quiesce_process_group(pg_id):
    """
    通用 Quiesce 函式：安全停止指定 Process Group 及其底下所有組件。
    奧卡姆剃刀原則：不封裝複雜類別，單一函式直達目的。
    """
    return nipyapi.canvas.schedule_process_group(process_group_id=pg_id, scheduled=False)