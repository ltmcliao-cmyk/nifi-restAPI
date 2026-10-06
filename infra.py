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

    # 3. 確保服務處於啟用狀態
    target_service = nipyapi.canvas.get_controller(target_service.id, 'id')
    if target_service.component.state != 'ENABLED':
        nipyapi.canvas.schedule_controller(target_service, scheduled=True)

    return target_service