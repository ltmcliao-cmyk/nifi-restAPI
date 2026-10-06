# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py
建置 Local_2_SQL Process Group 與 PostgreSQL 寫入流程。
包含舊群組安全清理、GetFile 本地取檔、RouteOnAttribute 分流以及 PutDatabaseRecord 寫入。
"""

import time
import nipyapi
from infra import init_json_reader


def purge_process_group_safely(pg_entity):
    """安全停止並刪除指定的 Process Group，避免 running / not disabled 衝突報錯。"""
    pg_id = pg_entity.id

    # 1. 停止該 Process Group 內所有的 Processor
    try:
        nipyapi.canvas.schedule_process_group(pg_id, scheduled=False)
        time.sleep(1)
    except Exception:
        pass

    # 2. 停用該 Process Group 內的所有 Controller Services
    try:
        controllers = nipyapi.canvas.list_all_controllers(pg_id)
        if controllers:
            for svc in controllers:
                if svc.component.state != 'DISABLED':
                    nipyapi.canvas.schedule_controller(svc, scheduled=False)
            time.sleep(1)
    except Exception:
        pass

    # 3. 刪除 Process Group
    try:
        nipyapi.canvas.delete_process_group(pg_entity, force=True)
        time.sleep(1)
    except Exception as e:
        print(f"Warning: Failed to purge old process group {pg_id}: {e}")


def create_local_2_sql_pg(parent_pg, dbcp_service, input_dir="/tmp/input", file_filter=".*\\.json"):
    """
    建立 Local_2_SQL Process Group，使用 GetFile 讀取檔案並寫入 PostgreSQL。
    
    :param parent_pg: 上層 Process Group (例如 Root Process Group)
    :param dbcp_service: 外部已初始化的 DBCPConnectionPool 服務實例
    :param input_dir: GetFile 監聽的本地資料夾路徑
    :param file_filter: GetFile 抓取的檔案檔名 Regular Expression
    """
    pg_name = "Local_2_SQL"

    # 1. 檢查並清理既有的同名 Process Group
    existing_pgs = nipyapi.canvas.list_all_process_groups(parent_pg.id)
    for pg in existing_pgs:
        if pg.component.name == pg_name:
            purge_process_group_safely(pg)

    # 2. 建立新的 Process Group
    local_pg = nipyapi.canvas.create_process_group(
        parent_pg=parent_pg,
        new_pg_name=pg_name,
        location=(400, 400)
    )

    # 3. 初始化 Process Group 內部的 JsonTreeReader Controller Service
    json_reader = init_json_reader(local_pg)

    # 4. 建立 GetFile 處理器 (取代原有的 Input Port)
    get_file = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=nipyapi.canvas.get_processor_type('GetFile'),
        location=(300, 100),
        name="Get Local JSON Files"
    )
    nipyapi.canvas.update_processor(
        get_file,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'Input Directory': input_dir,
                'File Filter': file_filter,
                'Keep Source File': 'false',
                'Recurse Subdirectories': 'true',
                'Minimum File Age': '0 sec'
            },
            scheduling_strategy='TIMER_DRIVEN',
            scheduling_period='10 sec'
        )
    )

    # 5. 建立 RouteOnAttribute 處理器 (分流/過濾)
    route_proc = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=nipyapi.canvas.get_processor_type('RouteOnAttribute'),
        location=(300, 300),
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
    # 重新獲取最新實體，確保 NiFi 生成的 'matched' Relationship 完成同步
    route_proc = nipyapi.canvas.get_processor(route_proc.id)

    # 6. 建立 PutDatabaseRecord 處理器 (寫入 PostgreSQL)
    put_db = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=nipyapi.canvas.get_processor_type('PutDatabaseRecord'),
        location=(300, 500),
        name="PutDatabaseRecord to PostgreSQL"
    )
    nipyapi.canvas.update_processor(
        put_db,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'Database Connection Pooling Service': dbcp_service.id,
                'Record Reader': json_reader.id,
                'Statement Type': 'INSERT',
                'Table Name': '${table_name}',
                'Schema Name': 'public',
                'Translate Field Names': 'true',
                'Unmatched Field Behavior': 'Ignore Unmatched Fields'
            },
            auto_terminated_relationships=['success', 'failure', 'retry']
        )
    )

    # 7. 連接各 Processor 資料流 (GetFile -> RouteOnAttribute -> PutDatabaseRecord)
    nipyapi.canvas.create_connection(
        source=get_file,
        target=route_proc,
        relationships=['success'],
        name="Files to Route"
    )

    nipyapi.canvas.create_connection(
        source=route_proc,
        target=put_db,
        relationships=['matched'],
        name="Matched to SQL"
    )

    return local_pg