# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py
建置 Local_2_SQL Process Group 與 PostgreSQL 寫入流程。
1. ListFile + FetchFile 安全讀取 Raw JSON。
2. DBCP 與 JsonTreeReader 建立在同一 Process Group 內部，確保 100% 成功綁定。
3. 自動啟用 Controller Services 並自動啟動全拓樸。
"""

import time
import nipyapi
from infra import init_json_reader, init_dbcp_pool


def purge_process_group_safely(pg_entity):
    """安全停止並刪除指定的 Process Group。"""
    pg_id = pg_entity.id

    try:
        nipyapi.canvas.schedule_process_group(pg_id, scheduled=False)
        time.sleep(1)
    except Exception:
        pass

    try:
        controllers = nipyapi.canvas.list_all_controllers(pg_id)
        if controllers:
            for svc in controllers:
                if svc.component.state != 'DISABLED':
                    nipyapi.canvas.schedule_controller(svc, scheduled=False)
            time.sleep(1)
    except Exception:
        pass

    try:
        nipyapi.canvas.delete_process_group(pg_entity, force=True)
        time.sleep(1)
    except Exception as e:
        print(f"Warning: Failed to purge old process group {pg_id}: {e}")


def create_local_2_sql_pg(parent_pg, db_config, input_dir="/opt/nifi/nifi-current/data/raw", file_filter=".*\\.json"):
    """
    建立 Local_2_SQL Process Group。
    傳入 db_config，直接在該 Process Group 內部初始化並啟用專屬的 DBCP 連線池。
    """
    pg_name = "Local_2_SQL"

    # 1. 清理既有同名群組
    existing_pgs = nipyapi.canvas.list_all_process_groups(parent_pg.id)
    for pg in existing_pgs:
        if pg.component.name == pg_name:
            purge_process_group_safely(pg)

    # 2. 建立新 Process Group
    local_pg = nipyapi.canvas.create_process_group(
        parent_pg=parent_pg,
        new_pg_name=pg_name,
        location=(400, 400)
    )

    # 3. 在同一 PG 內初始化 JsonTreeReader 與 DBCP 連線池 (徹底解決跨層無法綁定問題)
    json_reader = init_json_reader(local_pg)
    dbcp_service = init_dbcp_pool(local_pg, db_config)

    # 4. 建立 ListFile 處理器
    list_file = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=nipyapi.canvas.get_processor_type('ListFile'),
        location=(300, 50),
        name="List Local Raw Files"
    )
    nipyapi.canvas.update_processor(
        list_file,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'Input Directory': input_dir,
                'Recurse Subdirectories': 'true',
                'File Filter': file_filter,
                'Minimum File Age': '0 sec'
            },
            scheduling_strategy='TIMER_DRIVEN',
            scheduling_period='10 sec'
        )
    )

    # 5. 建立 FetchFile 處理器
    fetch_file = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=nipyapi.canvas.get_processor_type('FetchFile'),
        location=(300, 220),
        name="Fetch Raw Content (Read Only)"
    )
    nipyapi.canvas.update_processor(
        fetch_file,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'File to Fetch': '${absolute.path}/${filename}',
                'Completion Strategy': 'None'
            },
            auto_terminated_relationships=['not.found', 'permission.denied', 'failure']
        )
    )

    # 6. 建立 RouteOnAttribute 處理器
    route_proc = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=nipyapi.canvas.get_processor_type('RouteOnAttribute'),
        location=(300, 400),
        name="Route Table Type"
    )
    nipyapi.canvas.update_processor(
        route_proc,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'Routing Strategy': 'Route to Property name',
                'matched': "${filename:endsWith('.json')}"
            },
            auto_terminated_relationships=['unmatched']
        )
    )

    # 7. 建立 PutDatabaseRecord 處理器 (同層引用已啟用的 Controller Services)
    put_db = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=nipyapi.canvas.get_processor_type('PutDatabaseRecord'),
        location=(300, 580),
        name="PutDatabaseRecord to PostgreSQL"
    )
    nipyapi.canvas.update_processor(
        put_db,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'Database Connection Pooling Service': dbcp_service.id,
                'Record Reader': json_reader.id,
                'Statement Type': 'INSERT',
                'Table Name': 'raw_bike_availability',
                'Schema Name': 'public',
                'Translate Field Names': 'true',
                'Unmatched Field Behavior': 'Ignore Unmatched Fields'
            },
            auto_terminated_relationships=['success', 'failure', 'retry']
        )
    )

    # 8. 連接拓樸路由
    nipyapi.canvas.create_connection(
        source=list_file,
        target=fetch_file,
        relationships=['success'],
        name="Listed to Fetch"
    )

    nipyapi.canvas.create_connection(
        source=fetch_file,
        target=route_proc,
        relationships=['success'],
        name="Fetched to Route"
    )

    updated_route = nipyapi.canvas.get_processor(route_proc.id, identifier_type='id')
    if isinstance(updated_route, list):
        updated_route = updated_route[0]

    nipyapi.canvas.create_connection(
        source=updated_route,
        target=put_db,
        relationships=['matched'],
        name="Matched to SQL"
    )

    return local_pg