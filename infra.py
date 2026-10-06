# -*- coding: utf-8 -*-
"""
infra.py
集中管理 Root Process Group 與共用 Controller Services。
包含 PostgreSQL DBCP 連線池與 JsonTreeReader 的建置與自動啟用。
"""

import time
import nipyapi


def init_dbcp_pool(root_pg, db_config):
    """
    建立並啟用 PostgreSQL DBCP 連線池 Controller Service。
    一字不漏填入 5 個核心參數並自動切換為 ENABLED 狀態。
    """
    service_name = "PostgreSQL_DBCP_Pool"

    # 1. 檢查是否已有同名 Service，若存在則先安全停用並刪除或重複使用
    existing_services = nipyapi.canvas.list_all_controllers(root_pg.id)
    if existing_services:
        for svc in existing_services:
            if svc.component.name == service_name:
                try:
                    if svc.component.state != 'DISABLED':
                        nipyapi.canvas.schedule_controller(svc, scheduled=False)
                        time.sleep(1)
                    nipyapi.canvas.delete_controller(svc)
                    time.sleep(1)
                except Exception:
                    pass

    # 2. 建立 DBCPConnectionPool Controller Service
    dbcp_type = nipyapi.canvas.get_controller_type('DBCPConnectionPool')
    dbcp_service = nipyapi.canvas.create_controller(
        parent_pg=root_pg,
        controller=dbcp_type,
        name=service_name
    )

    # 3. 填入 5 個核心連線參數 (同時支援 NiFi 1.12.1 內部識別碼與 UI 鍵名)
    nipyapi.canvas.update_controller(
        dbcp_service,
        nipyapi.nifi.ControllerServiceDTO(
            properties={
                # 必填 1: Database Connection URL
                'Database Connection URL': db_config['url'],
                'db-connection-url': db_config['url'],

                # 必填 2: Database Driver Class Name
                'Database Driver Class Name': db_config['driver_class'],
                'db-driver-class-name': db_config['driver_class'],

                # 必填 3: Database Driver Location(s)
                'Database Driver Location(s)': db_config['driver_location'],
                'db-driver-locations': db_config['driver_location'],

                # 必填 4: Database User
                'Database User': db_config['user'],
                'db-user': db_config['user'],

                # 必填 5: Password
                'Password': db_config['password'],
                'db-password': db_config['password']
            }
        )
    )

    # 4. 自動點擊閃電啟用 (SCHEDULE ENABLED)
    dbcp_service = nipyapi.canvas.get_controller(dbcp_service.id, identifier_type='id')
    try:
        nipyapi.canvas.schedule_controller(dbcp_service, scheduled=True)
        time.sleep(1)
    except Exception as e:
        print(f"Warning during DBCP enabling: {e}")

    return dbcp_service


def init_json_reader(parent_pg):
    """建立並啟用 JsonTreeReader Controller Service。"""
    service_name = "JsonTreeReader_Local"

    existing_services = nipyapi.canvas.list_all_controllers(parent_pg.id)
    if existing_services:
        for svc in existing_services:
            if svc.component.name == service_name:
                try:
                    if svc.component.state != 'DISABLED':
                        nipyapi.canvas.schedule_controller(svc, scheduled=False)
                        time.sleep(1)
                    nipyapi.canvas.delete_controller(svc)
                    time.sleep(1)
                except Exception:
                    pass

    reader_type = nipyapi.canvas.get_controller_type('JsonTreeReader')
    json_reader = nipyapi.canvas.create_controller(
        parent_pg=parent_pg,
        controller=reader_type,
        name=service_name
    )

    # 自動啟用
    json_reader = nipyapi.canvas.get_controller(json_reader.id, identifier_type='id')
    try:
        nipyapi.canvas.schedule_controller(json_reader, scheduled=True)
        time.sleep(1)
    except Exception as e:
        print(f"Warning during JsonReader enabling: {e}")

    return json_reader