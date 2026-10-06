# -*- coding: utf-8 -*-
"""infra.py - 基礎架構服務層

設計精神：
1. 第一性原理：集中管理 Root PG 的基礎連線與 Reader 服務。
2. 冪等性與複用：若服務已存在且可用則直接啟用並複用，避免因引用鎖定導致刪除失敗。
3. 高階封裝：使用 nipyapi 原生介面，完全不暴露底層 DTO 細節。
"""

import time
import nipyapi


def init_dbcp_pool(root_pg, db_config):
    """建立或複用並自動啟用 PostgreSQL DBCP 連線池 Controller Service。"""
    service_name = "PostgreSQL_DBCP_Pool"

    # 1. 檢查既有同名服務，若已存在則直接啟用並回傳（確保冪等性）
    existing_services = nipyapi.canvas.list_all_controllers(root_pg.id)
    if existing_services:
        for svc in existing_services:
            if svc.component.name == service_name:
                svc = nipyapi.canvas.get_controller(
                    svc.id, identifier_type="id"
                )
                if isinstance(svc, list):
                    svc = svc[0]
                if svc.component.state != "ENABLED":
                    nipyapi.canvas.schedule_controller(svc, scheduled=True)
                return svc

    # 2. 獲取 Controller Type
    controller_type = nipyapi.canvas.get_controller_type("DBCPConnectionPool")
    if isinstance(controller_type, list):
        # 確保選取的是標準 DBCPConnectionPool 而非 Lookup
        controller_type = next(
            (
                c
                for c in controller_type
                if c.type == "org.apache.nifi.dbcp.DBCPConnectionPool"
            ),
            controller_type[0],
        )

    # 3. 建立 Controller Service 實體
    dbcp_service = nipyapi.canvas.create_controller(
        parent_pg=root_pg, controller=controller_type, name=service_name
    )

    # 4. 同步最新 Revision 避免 400 衝突
    dbcp_service = nipyapi.canvas.get_controller(
        dbcp_service.id, identifier_type="id"
    )
    if isinstance(dbcp_service, list):
        dbcp_service = dbcp_service[0]

    # 5. 更新連線屬性
    dbcp_service.component.properties = {
        "Database Connection URL": db_config["url"],
        "Database Driver Class Name": db_config["driver_class"],
        "Database Driver Location(s)": db_config["driver_location"],
        "Database User": db_config["user"],
        "Password": db_config["password"],
    }
    dbcp_service = nipyapi.canvas.update_controller(
        dbcp_service, dbcp_service.component
    )

    # 6. 自動排程啟用
    dbcp_service = nipyapi.canvas.get_controller(
        dbcp_service.id, identifier_type="id"
    )
    if isinstance(dbcp_service, list):
        dbcp_service = dbcp_service[0]

    nipyapi.canvas.schedule_controller(dbcp_service, scheduled=True)
    return dbcp_service


def init_json_reader(parent_pg):
    """建立或複用並自動啟用 JsonTreeReader Controller Service。"""
    service_name = "JsonTreeReader_Local"

    # 1. 檢查既有同名服務，若已存在則啟用並回傳
    existing_services = nipyapi.canvas.list_all_controllers(parent_pg.id)
    if existing_services:
        for svc in existing_services:
            if svc.component.name == service_name:
                svc = nipyapi.canvas.get_controller(
                    svc.id, identifier_type="id"
                )
                if isinstance(svc, list):
                    svc = svc[0]
                if svc.component.state != "ENABLED":
                    nipyapi.canvas.schedule_controller(svc, scheduled=True)
                return svc

    # 2. 獲取 Controller Type
    reader_type = nipyapi.canvas.get_controller_type("JsonTreeReader")
    if isinstance(reader_type, list):
        reader_type = reader_type[0]

    # 3. 建立 Controller Service 實體
    json_reader = nipyapi.canvas.create_controller(
        parent_pg=parent_pg, controller=reader_type, name=service_name
    )

    # 4. 自動排程啟用
    json_reader = nipyapi.canvas.get_controller(
        json_reader.id, identifier_type="id"
    )
    if isinstance(json_reader, list):
        json_reader = json_reader[0]

    nipyapi.canvas.schedule_controller(json_reader, scheduled=True)
    return json_reader