# main.py
import os
import time
import nipyapi
from pgs import local_2_SQL, flowfile_generator
import routes

def cleanup_junk_and_old_generators(root_id, keep_pg_id=None):
    """清除 Root 上所有散落元件，以及除 keep_pg_id 以外的所有 Process Group"""
    # 1. 清除 Root 層殘留的連線與處理器
    for conn in nipyapi.canvas.list_all_connections(root_id):
        try:
            nipyapi.canvas.purge_connection(conn.id)
            nipyapi.canvas.delete_connection(conn)
        except Exception:
            pass

    for proc in nipyapi.canvas.list_all_processors(root_id):
        try:
            nipyapi.canvas.schedule_processor(proc, False)
            nipyapi.canvas.delete_processor(proc)
        except Exception:
            pass

    # 2. 清除除了保留群組以外的所有其他 Process Group (包含歷史重複的 Generator)
    for pg in nipyapi.canvas.list_all_process_groups(root_id):
        if keep_pg_id and pg.id == keep_pg_id:
            continue
        # 遞迴關閉並刪除舊 PG
        try:
            nipyapi.canvas.schedule_process_group(pg.id, False)
            # 清理內部連線佇列後刪除
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

    # 1. 建立或鎖定你要保留的 Local_2_SQL PG
    local_pg = local_2_SQL.create_local_2_sql_pg(
        parent_pg=root_pg,
        db_config=db_config,
        table_name="raw_bike_availability",
        input_dir=raw_data_dir,
        file_filter=".*",
    )

    # 2. 核心：只保留 local_pg，其他歷史殘留 Process Group 與 Root 垃圾全部刪除
    cleanup_junk_and_old_generators(root_id=root_pg.id, keep_pg_id=local_pg.id)

    # 3. 重新建立全新的 FlowFile_Generator (獨立群組，內部產生資料)
    generator_pg = flowfile_generator.create_flowfile_generator_pg(parent_pg=root_pg)

    # 4. 連線路由
    if hasattr(routes, "setup_inter_pg_routes"):
        routes.setup_inter_pg_routes(local_pg=local_pg, generator_pg=generator_pg)
    elif hasattr(routes, "create_inter_pg_routes"):
        routes.create_inter_pg_routes(local_pg=local_pg, generator_pg=generator_pg)

    # 5. 啟動群組
    nipyapi.canvas.schedule_process_group(local_pg.id, scheduled=True)
    nipyapi.canvas.schedule_process_group(generator_pg.id, scheduled=True)

    # 6. 把 generator 裡的 Sink 停住，讓 Queue 持續堆積在它的專屬群組內
    for proc in nipyapi.canvas.list_all_processors(generator_pg.id):
        if proc.component.name == "Queue_Sink_Stopped":
            nipyapi.canvas.schedule_processor(proc, scheduled=False)

    print(f"[OK] 畫布清理完成，目前只保留: '{local_pg.component.name}' 與全新的 '{generator_pg.component.name}'")


if __name__ == "__main__":
    main()