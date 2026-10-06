# -*- coding: utf-8 -*-
import nipyapi

def init_dbcp_pool(parent_pg, db_config):
    """
    建立並確保 PostgreSQL DBCPConnectionPool Controller Service 可用。
    若已存在且啟用，直接沿用；若未啟用則啟用它。
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

        # 填入連線屬性
        nipyapi.canvas.update_controller(
            controller=target_service,
            update=nipyapi.nifi.ControllerServiceDTO(
                properties={
                    'Database Connection URL': f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['db_name']}",
                    'Database Driver Class Name': 'org.postgresql.Driver',
                    'Database Driver Location(s)': db_config.get('driver_location', ''),
                    'Database User': db_config['user'],
                    'Password': db_config['password'],
                    'Max Total Connections': str(db_config.get('max_conn', '8'))
                }
            )
        )

    # 3. 確保 Controller Service 已啟用
    if target_service.component.state != 'ENABLED':
        nipyapi.canvas.schedule_controller_service(target_service, scheduled=True)

    return target_service


def init_json_reader(parent_pg):
    """
    建立並確保 JsonTreeReader Controller Service 可用。
    若已存在且啟用，直接沿用；若未啟用則啟用它。
    """
    reader_name = "JsonTreeReader_Default"

    # 1. 檢查是否已存在同名 Controller Service
    existing_services = nipyapi.canvas.list_all_controllers(parent_pg.id)
    target_service = None
    if existing_services:
        for svc in existing_services:
            if svc.component.name == reader_name:
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
            name=reader_name
        )

    # 3. 確保 Controller Service 已啟用
    if target_service.component.state != 'ENABLED':
        nipyapi.canvas.schedule_controller_service(target_service, scheduled=True)

    return target_service