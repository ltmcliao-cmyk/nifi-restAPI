# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py
建置 Local_2_SQL Process Group 與 PostgreSQL 寫入流程。
採用工業標準 ListFile + FetchFile 架構：
1. 嚴格保護 Raw Data，絕不刪除或移動原始檔案。
2. 支援 Docker 容器 :ro 唯讀掛載。
3. 自動遞迴掃描子資料夾，並透過 State 增量追蹤避免重複抓取。
4. 完整相容 NiFi 1.12.1 PutDatabaseRecord 內部 Descriptor 命名規則。
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


def create_local_2_sql_pg(parent_pg, dbcp_service, input_dir="/opt/nifi/nifi-current/data/raw", file_filter=".*\\.json"):
    """
    建立 Local_2_SQL Process Group。
    """
    pg_name = "Local_2_SQL"

    # 1. 清理舊群組
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

    # 3. 初始化 JsonTreeReader
    json_reader = init_json_reader(local_pg)

    # 4. 建立 ListFile 處理器 (純粹掃描目錄並維持狀態，不碰實體檔案)
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
                'Minimum File Age': '0 sec',
                'Include Directed Relative Path': 'true'
            },
            scheduling_strategy='TIMER_DRIVEN',
            scheduling_period='10 sec'
        )
    )

    # 5. 建立 FetchFile 處理器 (安全讀取檔案內容，明確宣告不刪除原檔)
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
                'Completion Strategy': 'None',       # 完全不刪除、不移動原始資料
                'Move Conflict Strategy': 'Do Not Rename'
            },
            auto_terminated_relationships=['not.found', 'permission.denied', 'failure']
        )
    )

    # 6. 建立 RouteOnAttribute 處理器 (分流/過濾)
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

    # 7. 建立 PutDatabaseRecord 處理器 (注入底層識別碼，修復 4 項必填驗證報錯)
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
                # 1. Database Connection Pooling Service (底層 ID: put-db-record-dps)
                'put-db-record-dps': dbcp_service.id,
                'Database Connection Pooling Service': dbcp_service.id,

                # 2. Record Reader (底層 ID: record-reader)
                'record-reader': json_reader.id,
                'Record Reader': json_reader.id,

                # 3. Statement Type (底層 ID: statement-type)
                'statement-type': 'INSERT',
                'Statement Type': 'INSERT',

                # 4. Table Name (底層 ID: table-name)
                'table-name': 'raw_bike_availability',
                'Table Name': 'raw_bike_availability',

                # Schema 設定
                'schema-name': 'public',
                'Schema Name': 'public',

                # 欄位映射與未匹配欄位行為
                'Translate Field Names': 'true',
                'Unmatched Field Behavior': 'Ignore Unmatched Fields'
            },
            auto_terminated_relationships=['success', 'failure', 'retry']
        )
    )

    # 8. 串接拓樸路由連線
    # ListFile (success) -> FetchFile
    nipyapi.canvas.create_connection(
        source=list_file,
        target=fetch_file,
        relationships=['success'],
        name="Listed to Fetch"
    )

    # FetchFile (success) -> RouteOnAttribute
    nipyapi.canvas.create_connection(
        source=fetch_file,
        target=route_proc,
        relationships=['success'],
        name="Fetched to Route"
    )

    # RouteOnAttribute (matched) -> PutDatabaseRecord
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