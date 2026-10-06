# -*- coding: utf-8 -*-
import nipyapi
from infra import init_dbcp_pool, init_json_reader

def build_local_2_sql_pg(parent_pg, db_config):
    """
    建立 local_2_SQL Process Group 與 PostgreSQL 寫入流程 (使用 PutDatabaseRecord)
    """
    # 建立 Process Group
    local_pg = nipyapi.canvas.create_process_group(
        parent_pg=parent_pg,
        new_pg_name="Local_2_SQL",
        location=(400.0, 400.0)
    )

    # 1. 初始化 Controller Services (DBCP 與 JsonTreeReader)
    dbcp_service = init_dbcp_pool(local_pg, db_config)
    json_reader_service = init_json_reader(local_pg)

    # 2. 建立 Input Port 或讀取來源 (依既有設計)
    input_port = nipyapi.canvas.create_port(
        parent_pg=local_pg,
        port_type='INPUT_PORT',
        name='In_Local_JSON',
        location=(100.0, 200.0)
    )

    # 3. 建立 RouteOnAttribute 分流器
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

    # 連接 Input Port -> RouteOnAttribute
    nipyapi.canvas.create_connection(
        source=input_port,
        target=route_on_attr,
        relationships=[]
    )

    # 4. 重新取得 RouteOnAttribute 物件以載入動態關係 (is_station / is_availability)
    route_on_attr = nipyapi.canvas.get_processor(route_on_attr.id)

    # 5. 建立 PutDatabaseRecord 處理器
    put_db_type = nipyapi.canvas.get_processor_type('PutDatabaseRecord')

    # Station 寫入處理器
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

    # Availability 寫入處理器
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

    # 6. 連接 RouteOnAttribute -> PutDatabaseRecord
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