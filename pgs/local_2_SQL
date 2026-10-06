# -*- coding: utf-8 -*-
"""
pgs/local_2_SQL.py - 本地機地端資料寫入 PostgreSQL 之 Process Group
設計邏輯與精神：
1. 第一性原理 (高內聚)：邊緣端 Pipeline 為一個獨立自治的 ETL 單元 [GetFile -> ConvertJSONToSQL -> PutSQL]。
   將內部 Processor 宣告與 FlowFile 連線 (Connections) 一併歸入本模組建置，符合模組化開發。
2. 操作第一層封裝：完全透過 nipyapi.canvas.create_process_group, create_processor, create_connection。
3. 奧卡姆剃刀：由單一 build 函式完成該 PG 內部元件與拓樸的串接，並直接回傳 local_pg 物件。
"""

import nipyapi

def build_local_2_sql_pg(parent_pg, dbcp_service, position=(400.0, 400.0)):
    """
    建置 local_2_SQL Process Group、內部 Processor 組件及其 FlowFile 路由拓樸。

    Args:
        parent_pg (ProcessGroupEntity): 父層 Process Group (通常為 Root)。
        dbcp_service (ControllerServiceEntity): 已啟用的 DBCP 連線池。
        position (tuple): 在 NiFi Canvas 上的坐標。

    Returns:
        ProcessGroupEntity: 建置與串串完成的 local_2_SQL Process Group 實例。
    """
    # 1. 建立獨立的 Process Group
    local_pg = nipyapi.canvas.create_process_group(
        parent_pg=parent_pg,
        new_pg_name="local_2_SQL",
        location=position,
        comment="邊緣端 YouBike TDX JSON 資料落盤至 PostgreSQL"
    )

    # 2. 獲取 Processor 抽象型別 (第一層封裝)
    getfile_type = nipyapi.canvas.get_processor_type('GetFile')
    convert_json_type = nipyapi.canvas.get_processor_type('ConvertJSONToSQL')
    putsql_type = nipyapi.canvas.get_processor_type('PutSQL')

    # 3. 實作 Processor 1: GetFile (讀取 fetch_tdx 產出的 JSON 檔案)
    get_file = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=getfile_type,
        location=(100.0, 100.0),
        name="Ingest_TDX_JSON"
    )
    nipyapi.canvas.update_processor(
        get_file,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'Input Directory': '/opt/nifi/nifi-current/data/raw',
                'File Filter': '.*\\.json$',
                'Keep Source File': 'false'
            },
            scheduling_period='10s'
        )
    )

    # 4. 實作 Processor 2: ConvertJSONToSQL (將 JSON 轉換為 SQL 語法)
    convert_json = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=convert_json_type,
        location=(100.0, 300.0),
        name="Convert_JSON_to_SQL"
    )
    nipyapi.canvas.update_processor(
        convert_json,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'JDBC Connection Pool': dbcp_service.id,
                'Statement Type': 'INSERT',
                'Table Name': 'youbike_station',
                'Catalog Name': 'pipeline_db'
            },
            auto_terminated_relationships=['failure', 'original']
        )
    )

    # 5. 實作 Processor 3: PutSQL (寫入 PostgreSQL)
    put_sql = nipyapi.canvas.create_processor(
        parent_pg=local_pg,
        processor=putsql_type,
        location=(100.0, 500.0),
        name="Execute_PutSQL"
    )
    nipyapi.canvas.update_processor(
        put_sql,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                'JDBC Connection Pool': dbcp_service.id
            },
            auto_terminated_relationships=['success', 'retry', 'failure']
        )
    )

    # 6. 內部 FlowFile 路由拓樸串接 (Ingest -> Transform -> Load)
    nipyapi.canvas.create_connection(
        source=get_file,
        target=convert_json,
        relationships=['success'],
        name='Raw_JSON_Flow'
    )

    nipyapi.canvas.create_connection(
        source=convert_json,
        target=put_sql,
        relationships=['sql'],
        name='Prepared_SQL_Flow'
    )

    return local_pg