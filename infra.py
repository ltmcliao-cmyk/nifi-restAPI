# -*- coding: utf-8 -*-
"""
infra.py - 基礎架構服務層
設計精神：
1. 高級封裝：調用 nipyapi 抽象介面，不暴露底層 DTO 與繁瑣的協議細節。
2. 自動自癒：既有同名服務自動清理，避免衝突。
3. 狀態確定性：建立即啟用，開箱即用。
"""

import time
import nipyapi


def init_dbcp_pool(root_pg, db_config):
    """
    建立並自動啟用 PostgreSQL DBCP 連線池 Controller Service。
    完全封裝 5 個連線參數，執行完畢即為綠燈 (ENABLED)。
    """
    service_name = "PostgreSQL_DBCP_Pool"

    # 1. 檢查並清理既有同名服務
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

    # 2. 獲取 Controller Type (nipyapi 官方頂層抽象函式)
    controller_type = nipyapi.canvas.get_controller_type('DBCPConnectionPool')
    if isinstance(controller_type, list):
        controller_type = controller_type[0]

    # 3. 建立 Controller Service 實體
    dbcp_service = nipyapi.canvas.create_controller(
        parent_pg=root_pg,
        controller=controller_type,
        name=service_name
    )

    # 4. 配置連線屬性 (直接傳入字典，nipyapi 自動處理底層封裝)
    nipyapi.canvas.update_controller(
        dbcp_service,
        nipyapi.nifi.ControllerServiceDTO(
            properties={
                'Database Connection URL': db_config['url'],
                'Database Driver Class Name': db_config['driver_class'],
                'Database Driver Location(s)': db_config['driver_location'],
                'Database User': db_config['user'],
                'Password': db_config['password']
            }
        )
    )

    # 5. 自動排程啟用 (Enable Controller Service)
    dbcp_service = nipyapi.canvas.get_controller(dbcp_service.id, identifier_type='id')
    if isinstance(dbcp_service, list):
        dbcp_service = dbcp_service[0]

    nipyapi.canvas.schedule_controller(dbcp_service, scheduled=True)
    return dbcp_service


def init_json_reader(parent_pg):
    """
    建立並自動啟用 JsonTreeReader Controller Service。
    """
    service_name = "JsonTreeReader_Local"

    # 1. 檢查並清理既有同名服務
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

    # 2. 獲取 Controller Type
    reader_type = nipyapi.canvas.get_controller_type('JsonTreeReader')
    if isinstance(reader_type, list):
        reader_type = reader_type[0]

    # 3. 建立 Controller Service 實體
    json_reader = nipyapi.canvas.create_controller(
        parent_pg=parent_pg,
        controller=reader_type,
        name=service_name
    )

    # 4. 自動排程啟用
    json_reader = nipyapi.canvas.get_controller(json_reader.id, identifier_type='id')
    if isinstance(json_reader, list):
        json_reader = json_reader[0]

    nipyapi.canvas.schedule_controller(json_reader, scheduled=True)
    return json_reader