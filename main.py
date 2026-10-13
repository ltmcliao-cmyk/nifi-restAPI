# main.py
import os
import nipyapi
from pgs import local_2_SQL, flowfile_generator
import routes


def cleanup_only_root_junk_and_old_generators(root_pg, keep_pg_id):
    """
    精確清理：
    1. 只刪除隸屬於 Root 畫布上的散落元件（不碰任何子 PG 內部元件）
    2. 刪除除了 keep_pg_id 以外的其他 Process Group（例如舊的 FlowFile_Generator）
    """
    root_id = root_pg.id

    # 1. 只刪除直接放在 Root 上的連線 (parent_group_id 為 root)
    for conn in nipyapi.canvas.list_all_connections(root_id):
        # 關鍵過濾：不是直接掛在 Root 上的連線絕對不碰！
        if conn.component.parent_group_id != root_id:
            continue
        try:
            nipyapi.canvas.purge_connection(conn.id)
            nipyapi.canvas.delete_connection(conn)
        except Exception:
            pass

    # 2. 只刪除直接放在 Root 上的散落處理器
    for proc in nipyapi.canvas.list_all_processors(root_id):
        if proc.component.parent_group_id != root_id:
            continue
        try:
            nipyapi.canvas.schedule_processor(proc, False)
            nipyapi.canvas.delete_processor(proc)
        except Exception:
            pass

    # 3. 刪除除了 keep_pg_id 之外的子 Process Group
    for pg in nipyapi.canvas.list_all_process_groups(root_id):
        if pg.id == keep_pg_id:
            continue
        try:
            # 停止並清空該群組
            nipyapi.canvas.schedule_process_group(pg.id, False)
            for sub_conn in nipyapi.canvas.list_all_connections(pg.id):
                nipyapi.canvas.purge_connection(sub_conn.id)
            nipyapi.canvas.delete_process_group(pg, refresh=True)
        except Exception:
            pass


def main():
    nifi_host = os.getenv("NIFI_HOST", "http://localhost:8080")
    nipyapi.config.nifi_config.host = f"{nifi_host.rstrip('/')}/nifi-api"

    root_id = nipyapi.canvas.get_root_pg_id()
    root_pg = nipyapi.canvas.get_process_group(root_id, identifier_type="id")

    db_config = {
        "url": "jdbc:postgresql://postgres:5432/pipeline_db",
        "driver_class": "org.postgresql.Driver",
        "driver_location": "/opt/nifi/nifi-current/drivers/postgresql-driver",
        "user": "postgres",
        "password": "postgrespassword123",
    }
    raw_data_dir = "/opt/nifi/nifi-current/data/raw"

    # 1. 建立或鎖定 Local_2_SQL PG (若內部缺件，函式會自動補齊)
    local_pg = local_2_SQL.create_local_2_sql_pg(
        parent_pg=root_pg,
        db_config=db_config,
        table_name="raw_bike_availability",
        input_dir=raw_data_dir,
        file_filter=".*",
    )

    # 2. 精確清理：只清 Root 與舊 generator，絕不誤傷 local_pg
    cleanup_only_root_junk_and_old_generators(root_pg=root_pg, keep_pg_id=local_pg.id)

    # 3. 建立全新的 FlowFile_Generator
    generator_pg = flowfile_generator.create_flowfile_generator_pg(parent_pg=root_pg)

    # 4. 連線路由
    if hasattr(routes, "setup_inter_pg_routes"):
        routes.setup_inter_pg_routes(local_pg=local_pg, generator_pg=generator_pg)
    elif hasattr(routes, "create_inter_pg_routes"):
        routes.create_inter_pg_routes(local_pg=local_pg, generator_pg=generator_pg)

    # 5. 啟動群組
    nipyapi.canvas.schedule_process_group(local_pg.id, scheduled=True)
    nipyapi.canvas.schedule_process_group(generator_pg.id, scheduled=True)

    # 6. 把 generator 裡的 Sink 停住，讓資料留存在 Queue
    for proc in nipyapi.canvas.list_all_processors(generator_pg.id):
        if proc.component.name == "Queue_Sink_Stopped":
            nipyapi.canvas.schedule_processor(proc, scheduled=False)

    print(f"[OK] 部署與防誤刪清理完成！保留: '{local_pg.component.name}' 與 '{generator_pg.component.name}'")


if __name__ == "__main__":
    main()