# -*- coding: utf-8 -*-
"""
main.py - 純粹的系統調度器 (簡化版)
設計邏輯與精神：
1. 第一性原理：main.py 的唯一職責是「順序調度」，完全無需關心 PG 內部的組件細節或連接細節。
2. 奧卡姆剃刀：移除了跨模組傳遞 processors 字典的中介變數，調度流程一目瞭然。
3. 流程導向：完全依循 [Endpoint -> Infra -> PG Build -> Inter-PG Routes -> Schedule] 執行。
"""

import nipyapi
import infra
from pgs import local_2_SQL
import routes

def main():
    # 1. 設定 NiFi API 連線端點 (第一層封裝設定)
    nipyapi.config.nifi_config.host = 'http://localhost:8080/nifi-api'

    # 2. 獲取 Canvas Root Process Group
    root_pg = nipyapi.canvas.get_process_group('root')

    # 3. 載入 Infra (建置 PostgreSQL DBCP 連線池)
    db_config = {
        'url': 'jdbc:postgresql://postgres:5432/pipeline_db',
        'driver_class': 'org.postgresql.Driver',
        'driver_location': '/opt/nifi/nifi-current/drivers/postgresql-42.7.3.jar',
        'user': 'postgres',
        'password': 'postgrespassword123'
    }
    dbcp_service = infra.init_dbcp_pool(root_pg, db_config)

    # 4. 載入 PG (建立 local_2_SQL 業務邏輯及其內部 FlowFile 拓樸)
    #    此處直接傳回 local_pg 實例，無需導出內部 processors 字典
    local_pg = local_2_SQL.build_local_2_sql_pg(root_pg, dbcp_service)

    # 5. 執行跨 PG (Port-to-Port) 拓樸路由 (目前為單一 PG 預留介面)
    routes.build_inter_pg_routes()

    # 6. 啟動 Process Group 開始運作
    nipyapi.canvas.schedule_process_group(local_pg.id, scheduled=True)
    print(f"[OK] Successfully initialized and started Process Group '{local_pg.component.name}' (ID: {local_pg.id})")

if __name__ == '__main__':
    main()