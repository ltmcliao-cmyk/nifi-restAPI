"""
main 模組 - 系統調度器
職責：
1. 使用 nipyapi 初始化並啟動 NiFi Process Groups
2. 啟動完成後，直接調用 monitor_publisher 執行 API 端點輪詢與 Firebase 推送
"""

import os
import nipyapi
from pgs import local_2_SQL, flowfile_generator
import routes

# 匯入監控與推送模組
import monitor_publisher


def main():
    # 1. 連線設定與環境變數對齊 (修復 nipyapi host 配置)
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

    # 6. 建立 Continuous FlowFile Generator 拓樸 (即時監控測試用)
    generator_pg = flowfile_generator.create_flowfile_generator_pg(
        parent_pg=root_pg
    )

    # 7. 跨 PG 路由設定 (修復路由方法呼叫，傳入剛剛建立的兩個 PG)
    if hasattr(routes, 'setup_inter_pg_routes'):
        routes.setup_inter_pg_routes(local_pg=local_pg, generator_pg=generator_pg)
    elif hasattr(routes, 'create_inter_pg_routes'):
        routes.create_inter_pg_routes(local_pg=local_pg, generator_pg=generator_pg)

    # 8. 一鍵啟動全拓樸 (修復排程調度目標 PG)
    nipyapi.canvas.schedule_process_group(local_pg.id, scheduled=True)
    nipyapi.canvas.schedule_process_group(generator_pg.id, scheduled=True)

    print(f"[OK] Successfully initialized and started Process Group '{local_pg.component.name}' (ID: {local_pg.id})")
    print(f"[OK] Successfully initialized and started Process Group '{generator_pg.component.name}' (ID: {generator_pg.id})")

    # 9. 拓樸建置完畢，交接給監控守護行程執行主迴圈
    print("[INFO] Handing over to NiFi Monitor Publisher Daemon...")
    monitor_publisher.main()


if __name__ == '__main__':
    main()