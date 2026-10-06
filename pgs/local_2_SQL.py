# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py
建置 Local_2_SQL Process Group 與 PostgreSQL 寫入流程。
包含舊群組安全清理、建立 Input Port、RouteOnAttribute 分流以及 PutDatabaseRecord 寫入。
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

    # 3. 重新取得實例後安全刪除
    target_pg = nipyapi.canvas.get_process_group(pg_id, identifier_type='id')
    if target_pg:
        nipyapi.canvas.delete_process_group(target_pg, force=True)


def build_local_2_sql_pg(parent_pg, dbcp_service):
    """建立 local_2_SQL Process Group 與資料寫入拓樸。"""
    target_pg_name = "Local_2_SQL"

    # 1. 冪等性處理：檢查畫布上是否已存在舊的 Local_2_SQL（不分大小寫），有則安全清理
    all_pgs = nipyapi.canvas.list_all_process_groups(parent_pg.id)
    if all_pgs:
        for pg in all_pgs:
            if pg.component.name.lower() == target_pg_name.lower():
                purge_process_group_safely(pg)

    # 2. 建立全新的 Process Group
    local_pg = nipyapi.canvas.create_process_group(
        parent_pg=parent_pg,
        new_pg_name=target_pg_name,
        location=(400.0, 400.0)
    )

    # 取得 DBCP ID
    dbcp_id = dbcp_service.id if hasattr(dbcp_service, 'id') else dbcp_service

    # 3. 初始化 Controller Services (JsonTreeReader)
    json_reader_service = init_json_reader(local_pg)
    reader_id = json_reader_service.id if hasattr(json_reader_service, 'id') else json_reader_service

    # 4. 建立 Input Port
    input_port = nipyapi.canvas.create_port(
        pg_id=local_pg.id,
        port_type='INPUT_PORT',
        name='In_Local_JSON',
        state='STOPPED',
        position=(100.0, 200.0)
    )

    # 5. 建立 RouteOnAttribute 分流器
    route_processor_type = nipyapi.canvas.get_processor_type('RouteOnAttribute')
    route_on_attr = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=route_processor_type,
        location=(400.0, 200.0),
        name="Route_TDX_Type"
    )

    nipyapi.canvas.update_processor(
        route_on_attr,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'Routing Strategy': 'Route to Property name',
                'is_station': '${filename:contains("Station")}',
                'is_availability': '${filename:contains("Availability")}'
            },
            auto_terminated_relationships=['unmatched']
        )
    )

    # 連接 Input Port -> RouteOnAttribute
    nipyapi.canvas.create_connection(
        source=input_port,
        target=route_on_attr
    )

    # 刷新 RouteOnAttribute 物件以載入動態 relationship
    route_on_attr = nipyapi.canvas.get_processor(route_on_attr.id, identifier_type='id')

    # 6. 建立 PutDatabaseRecord 處理器
    put_db_type = nipyapi.canvas.get_processor_type('PutDatabaseRecord')

    # PutDatabaseRecord 通用屬性配置
    common_db_properties = {
        'record-reader': reader_id,
        'Database Connection Pooling Service': dbcp_id,
        'statement-type': 'INSERT',
        'db-type': 'PostgreSQL',
        'schema-name': 'public',
        'Translate Field Names': 'true',
        'Unmatched Field Behavior': 'Ignore Unmatched Fields'
    }

    # Station 寫入處理器
    put_db_station = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=put_db_type,
        location=(800.0, 100.0),
        name="PutDatabaseRecord_Station"
    )

    station_props = common_db_properties.copy()
    station_props['table-name'] = 'raw_station'

    nipyapi.canvas.update_processor(
        put_db_station,
        nipyapi.nifi.ProcessorConfigDTO(
            properties=station_props,
            auto_terminated_relationships=['success', 'failure', 'retry']
        )
    )

    # Availability 寫入處理器
    put_db_avail = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=put_db_type,
        location=(800.0, 300.0),
        name="PutDatabaseRecord_Availability"
    )

    avail_props = common_db_properties.copy()
    avail_props['table-name'] = 'raw_availability'

    nipyapi.canvas.update_processor(
        put_db_avail,
        nipyapi.nifi.ProcessorConfigDTO(
            properties=avail_props,
            auto_terminated_relationships=['success', 'failure', 'retry']
        )
    )

    # 7. 建立連接線
    nipyapi.canvas.create_connection(
        source=route_on_attr,
        target=put_db_station,
        relationships=['is_station'],
        name="Station_Stream"
    )

    nipyapi.canvas.create_connection(
        source=route_on_attr,
        target=put_db_avail,
        relationships=['is_availability'],
        name="Availability_Stream"
    )

    return local_pg