# -*- coding: utf-8 -*-
"""
main.py - 系統調度器
"""

import os
import nipyapi
import infra
from pgs import local_2_SQL
import routes


def main():
    # 1. 連線設定
    nifi_host = os.getenv('NIFI_HOST', 'http://localhost:8080')
    nipyapi.config.nifi_config.host = f"{nifi_host.rstrip('/')}/nifi-api"

    # 2. 取得 Root PG
    root_pg = nipyapi.canvas.get_process_group('root')

    # 3. 載入並啟用 PostgreSQL DBCP (精準填入 5 個核心參數)
    db_config = {
        'url': 'jdbc:postgresql://postgres:5432/pipeline_db',
        'driver_class': 'org.postgresql.Driver',
        'driver_location': '/opt/nifi/nifi-current/drivers/postgresql-42.7.3.jar',
        'user': 'postgres',
        'password': 'postgrespassword123'
    }
    dbcp_service = infra.init_dbcp_pool(root_pg, db_config)

    # 4. 指定容器內的唯讀掛載路徑
    raw_data_dir = "/opt/nifi/nifi-current/data/raw"

    # 5. 建立 Local_2_SQL 拓樸
    local_pg = local_2_SQL.create_local_2_sql_pg(
        parent_pg=root_pg,
        dbcp_service=dbcp_service,
        input_dir=raw_data_dir,
        file_filter=".*\\.json"
    )

    # 6. 路由保留介面
    routes.build_inter_pg_routes()

    # 7. 一鍵啟動全拓樸 (Controller Service 已先行啟用，此處將全綠燈運行)
    nipyapi.canvas.schedule_process_group(local_pg.id, scheduled=True)
    print(f"[OK] Successfully initialized and started Process Group '{local_pg.component.name}' (ID: {local_pg.id})")


if __name__ == '__main__':
    main()
