#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import nipyapi

# 引用專案根目錄現有的 infra 模組
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    import infra
except ImportError:
    infra = None

PROCESS_GROUP_NAME = "Local_2_SQL"
PUT_DB_PROCESSOR_NAME = "PutDatabaseRecord to PostgreSQL"


def configure_put_database_record(
    pg_entity, dbcp_service_id: str, reader_service_id: str, table_name: str
):
    """使用 nipyapi.canvas 第一層封裝修復 PutDatabaseRecord 處理器屬性"""
    # 透過 canvas 第一層封裝取得處理器
    processors = nipyapi.canvas.list_all_processors(pg_entity.id)
    target_proc = next(
        (p for p in processors if p.component.name == PUT_DB_PROCESSOR_NAME),
        None,
    )

    if not target_proc:
        raise ValueError(
            f"在 Process Group {pg_entity.id} 中找不到處理器: {PUT_DB_PROCESSOR_NAME}"
        )

    # 局部更新配置，補足驗證錯誤所缺的 4 項必要屬性與終止路由
    config = target_proc.component.config
    config.properties["Record Reader"] = reader_service_id
    config.properties["Database Connection Pooling Service"] = dbcp_service_id
    config.properties["Statement Type"] = "INSERT"
    config.properties["Table Name"] = table_name
    config.auto_terminated_relationships = ["success", "failure", "retry"]

    # 透過 canvas.update_processor 寫回 NiFi
    updated_proc = nipyapi.canvas.update_processor(target_proc, config)
    return updated_proc


def create_local_2_sql_pg(parent_pg=None, **kwargs):
    """
    提供給 main.py 第 33 行呼叫的標準入口函式。
    完全使用 nipyapi.canvas 第一層封裝，並回傳原生 ProcessGroupEntity 物件。
    """
    # 1. 取得上層 Process Group (若未傳入則預設為 Root)
    if not parent_pg:
        root_id = nipyapi.canvas.get_root_pg_id()
        parent_pg = nipyapi.canvas.get_process_group(root_id, "id")
    elif isinstance(parent_pg, str):
        parent_pg = nipyapi.canvas.get_process_group(parent_pg, "id")

    # 2. 取得或建立 Local_2_SQL Process Group
    local_pg = nipyapi.canvas.get_process_group(
        PROCESS_GROUP_NAME, identifier_type="name"
    )
    if not local_pg:
        local_pg = nipyapi.canvas.create_process_group(
            parent_pg=parent_pg,
            new_pg_name=PROCESS_GROUP_NAME,
            location=(0, 0),
        )

    # 3. 透過現有的 infra 模組確保 Controller Services 就緒
    if infra and hasattr(infra, "setup_infra"):
        infra_services = infra.setup_infra(local_pg.id)
        dbcp_id = infra_services.get("dbcp_id")
        reader_id = infra_services.get("reader_id")
        table_name = getattr(infra, "TABLE_NAME", "target_table")
    else:
        # 若 infra 為變數定義形式，直接讀取現有模組常數
        dbcp_id = getattr(infra, "DBCP_ID", None)
        reader_id = getattr(infra, "READER_ID", None)
        table_name = getattr(infra, "TABLE_NAME", "target_table")

    # 4. 修復 PutDatabaseRecord 驗證錯誤
    if dbcp_id and reader_id:
        configure_put_database_record(
            local_pg,
            dbcp_service_id=dbcp_id,
            reader_service_id=reader_id,
            table_name=table_name,
        )

    # 5. 回傳原生 ProcessGroupEntity，供 main.py 第 44 行使用 local_pg.id 排程
    return local_pg