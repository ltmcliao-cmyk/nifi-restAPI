""" main 模組 - 系統調度器 """
import os
import nipyapi
from pgs import local_2_SQL
from pgs import flowfile_generator  # 【新增】：導入新的 FlowFile 產生器模組
import routes

def main():
    # 1. 連線設定
    nifi_host = os.getenv('NIFI_HOST', 'http://localhost:8080')
    nipyapi.config.nifi_config.host = f"{nifi_host.rstrip('/')}/nifi-api"

    # 2. 取得 Root PG
    root_id = nipyapi.canvas.get_root_pg_id()
    root_pg = nipyapi.canvas.get_process_group(root_id, identifier_type='id')

    # 3. 資料庫連線配置
    db_config = {
        'url': 'jdbc:postgresql://postgres:5432/pipeline_db',
        'driver_class': 'org.postgresql.Driver',
        'driver_location': '/opt/nifi/nifi-current/drivers/postgresql-driver',
        'user': 'postgres',
        'password': 'postgrespassword123'
    }

    # 4. 指定容器內的唯讀掛載路徑
    raw_data_dir = "/opt/nifi/nifi-current/data/raw"

    # 5. 建立 Local_2_SQL 拓樸
    local_pg = local_2_SQL.create_local_2_sql_pg(
        parent_pg=root_pg,
        db_config=db_config,
        table_name="raw_bike_availability",
        input_dir=raw_data_dir,
        file_filter=".*"
    )

    # 【新增】：6. 建立 Continuous FlowFile Generator 拓樸 (即時監控測試用)
    generator_pg = flowfile_generator.create_flowfile_generator_pg(
        parent_pg=root_pg
    )

    # 7. 路由保留介面
    routes.build_inter_pg_routes()

    # 8. 一鍵啟動全拓樸 (同時調度 local_pg 與 generator_pg)
    nipyapi.canvas.schedule_process_group(local_pg.id, scheduled=True)
    nipyapi.canvas.schedule_process_group(generator_pg.id, scheduled=True)  # 【新增】：啟動測試資料產生器 Process Group

    print(f"[OK] Successfully initialized and started Process Group '{local_pg.component.name}' (ID: {local_pg.id})")
    print(f"[OK] Successfully initialized and started Process Group '{generator_pg.component.name}' (ID: {generator_pg.id})")

if __name__ == '__main__':
    main()