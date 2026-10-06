# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py
建置 Local_2_SQL Process Group：
透過 Input Port 接收外部 FlowFile，經 RouteOnAttribute 分流後，
透過 PutDatabaseRecord 寫入 PostgreSQL。
"""
import nipyapi
from infra import init_dbcp_pool, init_json_reader


def build_local_2_sql_pg(parent_pg, db_config):
    """
    建立 local_2_SQL Process Group 與 PostgreSQL 寫入流程。
    回傳建立的 (local_pg, input_port) 供 routes.py 或上層模組建立跨邊界連線。
    """
    # 1. 建立 Process Group
    local_pg = nipyapi.canvas.create_process_group(
        parent_pg=parent_pg,
        new_pg_name="Local_2_SQL",
        location=(400.0, 400.0)
    )

    # 2. 初始化 Controller Services
    dbcp_service = init_dbcp_pool(local_pg, db_config)
    json_reader_service = init_json_reader(local_pg)

    # 3. 建立群組對外的 Input Port (使用 pg_id 參數)
    input_port = nipyapi.canvas.create_port(
        pg_id=local_pg.id,
        port_type='INPUT_PORT',
        name='In_Local_JSON',
        location=(100.0, 200.0)
    )

    # 4. 建立 RouteOnAttribute 分流器
    route_processor_type = nipyapi.canvas.get_processor_type('RouteOnAttribute')
    route_on_attr = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=route_processor_type,
        location=(400.0, 200.0),
        name="Route_TDX_Type",
        config=nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'Routing Strategy': 'Route to Property name',
                'is_station': '${filename:contains("Station")}',
                'is_availability': '${filename:contains("Availability")}'
            },
            auto_terminated_relationships=['unmatched']
        )
    )

    # 5. 連接 Input Port -> RouteOnAttribute (Input Port 作為 source 時不需 relationships 參數)
    nipyapi.canvas.create_connection(
        source=input_port,
        target=route_on_attr
    )

    # 重新取得 RouteOnAttribute 物件以載入動態 relationship
    route_on_attr = nipyapi.canvas.get_processor(route_on_attr.id, identifier_type='id')

    # 6. 建立 PutDatabaseRecord
    put_db_type = nipyapi.canvas.get_processor_type('PutDatabaseRecord')

    put_db_station = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=put_db_type,
        location=(800.0, 100.0),
        name="PutDatabaseRecord_Station",
        config=nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'record-reader': json_reader_service.id,
                'Database Connection Pooling Service': dbcp_service.id,
                'db-type': 'PostgreSQL',
                'statement-type': 'INSERT',
                'table-name': 'raw_station',
                'schema-name': 'public',
                'Translate Field Names': 'true',
                'Unmatched Field Behavior': 'Ignore Unmatched Fields'
            },
            auto_terminated_relationships=['success', 'failure', 'retry']
        )
    )

    put_db_avail = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=put_db_type,
        location=(800.0, 300.0),
        name="PutDatabaseRecord_Availability",
        config=nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'record-reader': json_reader_service.id,
                'Database Connection Pooling Service': dbcp_service.id,
                'db-type': 'PostgreSQL',
                'statement-type': 'INSERT',
                'table-name': 'raw_availability',
                'schema-name': 'public',
                'Translate Field Names': 'true',
                'Unmatched Field Behavior': 'Ignore Unmatched Fields'
            },
            auto_terminated_relationships=['success', 'failure', 'retry']
        )
    )

    # 7. 連接 RouteOnAttribute -> PutDatabaseRecord
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

    return local_pg, input_port