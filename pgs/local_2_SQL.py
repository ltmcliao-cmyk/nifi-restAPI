"""
pgs/local_2_SQL.py
建置 local_2_SQL Process Group：
從本機掛載目錄遞迴讀取 TDX JSON（包含 station 與 availability），
分流後以 JSONB 形式直接落盤至 PostgreSQL raw 表。
"""

import nipyapi


def create_local_2_sql_pg(parent_pg, position=(100.0, 100.0), db_controller_id=None):
    """建置並配置 local_2_SQL Process Group。

    Args:
        parent_pg (ProcessGroupEntity): 父層 Process Group 實例。
        position (tuple): 在 NiFi Canvas 上的坐標。
        db_controller_id (str, optional): DBCPConnectionPool 服務 ID 或 Controller 實例。

    Returns:
        ProcessGroupEntity: 建置與串接完成的 local_2_SQL Process Group 實例。
    """
    # 支援傳入 ControllerServiceEntity 或純 ID 字串
    if db_controller_id and hasattr(db_controller_id, 'id'):
        db_controller_id = db_controller_id.id

    # 1. 建立獨立的 Process Group
    local_pg = nipyapi.canvas.create_process_group(
        parent_pg=parent_pg,
        new_pg_name="local_2_SQL",
        location=position,
        comment="TDX JSON (Station & Availability) 落盤至 PostgreSQL JSONB RAW 表"
    )

    # 2. 獲取 Processor 抽象型別
    getfile_type = nipyapi.canvas.get_processor_type('GetFile')
    route_type = nipyapi.canvas.get_processor_type('RouteOnAttribute')
    putsql_type = nipyapi.canvas.get_processor_type('PutSQL')

    # --------------------------------------------------------------------------
    # 3. Processor 1: GetFile (讀取 raw/ 下所有 JSON 檔案)
    # --------------------------------------------------------------------------
    get_file = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=getfile_type,
        location=(100.0, 100.0),
        name="Ingest_TDX_All_JSON"
    )

    nipyapi.canvas.update_processor(
        get_file,
        nipyapi.nifi.ProcessorConfigDTO(
            scheduling_period='10s',
            properties={
                'Input Directory': '/opt/nifi/nifi-current/data/raw',
                'Recurse Subdirectories': 'true',          # 遍歷 station/ 與 availability/ 子目錄
                'File Filter': r'.*\.json$',              # 同時接收 station 與 availability
                'Keep Source File': 'true',               # compose 掛載為 :ro，必須保留檔案
                'Minimum File Age': '5 sec'               # 避免讀取到尚未寫入完成的檔案
            },
            auto_terminated_relationships=[]
        )
    )

    # --------------------------------------------------------------------------
    # 4. Processor 2: RouteOnAttribute (根據檔名將 station 與 availability 分流)
    # --------------------------------------------------------------------------
    route_on_attr = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=route_type,
        location=(100.0, 300.0),
        name="Route_By_Category"
    )

    nipyapi.canvas.update_processor(
        route_on_attr,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'Routing Strategy': 'Route to Property name',
                'is_station': '${filename:startsWith("station")}',
                'is_availability': '${filename:startsWith("availability")}'
            },
            auto_terminated_relationships=['unmatched']
        )
    )

    # 關鍵修正：重新取得更新後的 processor 物件，載入動態產生的 is_station / is_availability 關聯
    route_on_attr = nipyapi.canvas.get_processor(route_on_attr.id, 'id')

    # --------------------------------------------------------------------------
    # 5. Processor 3: PutSQL for Station (寫入 raw_bike_station)
    # --------------------------------------------------------------------------
    put_sql_station = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=putsql_type,
        location=(0.0, 500.0),
        name="PutSQL_Raw_Station"
    )

    station_properties = {
        'SQL Statement': 'INSERT INTO raw_bike_station (payload) VALUES (?::jsonb);',
        'Support Fragmented Transactions': 'false',
        'Batch Size': '100'
    }
    if db_controller_id:
        station_properties['JDBC Connection Pool'] = db_controller_id

    nipyapi.canvas.update_processor(
        put_sql_station,
        nipyapi.nifi.ProcessorConfigDTO(
            properties=station_properties,
            auto_terminated_relationships=['success', 'failure', 'retry']
        )
    )

    # --------------------------------------------------------------------------
    # 6. Processor 4: PutSQL for Availability (寫入 raw_bike_availability)
    # --------------------------------------------------------------------------
    put_sql_avail = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=putsql_type,
        location=(250.0, 500.0),
        name="PutSQL_Raw_Availability"
    )

    avail_properties = {
        'SQL Statement': 'INSERT INTO raw_bike_availability (payload) VALUES (?::jsonb);',
        'Support Fragmented Transactions': 'false',
        'Batch Size': '100'
    }
    if db_controller_id:
        avail_properties['JDBC Connection Pool'] = db_controller_id

    nipyapi.canvas.update_processor(
        put_sql_avail,
        nipyapi.nifi.ProcessorConfigDTO(
            properties=avail_properties,
            auto_terminated_relationships=['success', 'failure', 'retry']
        )
    )

    # --------------------------------------------------------------------------
    # 7. 建立連接線 (Connections)
    # --------------------------------------------------------------------------
    # GetFile -> RouteOnAttribute
    nipyapi.canvas.create_connection(
        source=get_file,
        target=route_on_attr,
        relationships=['success'],
        name="All_JSON_Stream"
    )

    # RouteOnAttribute (is_station) -> PutSQL_Raw_Station
    nipyapi.canvas.create_connection(
        source=route_on_attr,
        target=put_sql_station,
        relationships=['is_station'],
        name="Station_Stream"
    )

    # RouteOnAttribute (is_availability) -> PutSQL_Raw_Availability
    nipyapi.canvas.create_connection(
        source=route_on_attr,
        target=put_sql_avail,
        relationships=['is_availability'],
        name="Availability_Stream"
    )

    return local_pg


def build_local_2_sql_pg(parent_pg, dbcp_service, position=(100.0, 100.0)):
    """
    提供給 main.py 呼叫的轉接函式，同時相容 dbcp_service 實例物件與 ID。
    """
    controller_id = dbcp_service.id if hasattr(dbcp_service, 'id') else dbcp_service
    return create_local_2_sql_pg(
        parent_pg=parent_pg,
        position=position,
        db_controller_id=controller_id
    )