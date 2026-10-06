#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import requests

NIFI_API_BASE = "http://127.0.0.1:8080/nifi-api"
PROCESS_GROUP_ID = "10d7800a-01a1-1000-e2d4-9b50ad6cf816"
PUT_DB_PROCESSOR_ID = "10d78562-01a1-1000-adec-5b289ce54b88"

# 請依實際環境填寫資料庫與表格設定
DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/your_database",
    "driver_class": "org.postgresql.Driver",
    "driver_location": "/opt/nifi/drivers/postgresql-42.2.14.jar",  # 填入容器或主機中 PostgreSQL JDBC JAR 的路徑
    "user": "postgres",
    "password": "your_password",
    "table_name": "target_table",  # 寫入目標資料表名稱
    "statement_type": "INSERT",  # INSERT, UPDATE, UPSERT 等
}


def get_pg_info(pg_id: str):
    """取得 Process Group 資訊與版本號"""
    resp = requests.get(f"{NIFI_API_BASE}/process-groups/{pg_id}")
    resp.raise_for_status()
    return resp.json()


def create_or_get_dbcp_service(pg_id: str) -> str:
    """在指定 Process Group 建立並啟用 PostgreSQL DBCPConnectionPool"""
    url = f"{NIFI_API_BASE}/process-groups/{pg_id}/controller-services"

    payload = {
        "revision": {"version": 0},
        "component": {
            "name": "PostgreSQL Connection Pool",
            "type": "org.apache.nifi.dbcp.DBCPConnectionPool",
            "properties": {
                "Database Connection URL": DB_CONFIG["url"],
                "Database Driver Class Name": DB_CONFIG["driver_class"],
                "Database Driver Location(s)": DB_CONFIG["driver_location"],
                "Database User": DB_CONFIG["user"],
                "Password": DB_CONFIG["password"],
            },
        },
    }

    resp = requests.post(url, json=payload)
    if resp.status_code == 201:
        cs_data = resp.json()
        cs_id = cs_data["id"]
        print(f"[+] DBCPConnectionPool 建立成功: {cs_id}")
    else:
        # 若已存在則取得列表
        services = requests.get(
            f"{NIFI_API_BASE}/flow/process-groups/{pg_id}/controller-services"
        ).json()
        for svc in services.get("controllerServices", []):
            if "DBCPConnectionPool" in svc["component"]["type"]:
                return svc["id"]
        resp.raise_for_status()

    # 啟用 Controller Service
    enable_controller_service(cs_id)
    return cs_id


def create_or_get_record_reader(pg_id: str, reader_type="json") -> str:
    """建立並啟用 Record Reader (預設 JsonTreeReader，亦可改為 CSVReader)"""
    service_type = (
        "org.apache.nifi.json.JsonTreeReader"
        if reader_type == "json"
        else "org.apache.nifi.csv.CSVReader"
    )
    service_name = (
        "Default JsonTreeReader"
        if reader_type == "json"
        else "Default CSVReader"
    )

    url = f"{NIFI_API_BASE}/process-groups/{pg_id}/controller-services"
    payload = {
        "revision": {"version": 0},
        "component": {"name": service_name, "type": service_type},
    }

    resp = requests.post(url, json=payload)
    if resp.status_code == 201:
        cs_data = resp.json()
        cs_id = cs_data["id"]
        print(f"[+] Record Reader 建立成功: {cs_id}")
    else:
        services = requests.get(
            f"{NIFI_API_BASE}/flow/process-groups/{pg_id}/controller-services"
        ).json()
        for svc in services.get("controllerServices", []):
            if reader_type in svc["component"]["type"].lower():
                return svc["id"]
        resp.raise_for_status()

    enable_controller_service(cs_id)
    return cs_id


def enable_controller_service(cs_id: str):
    """啟用指定 Controller Service"""
    detail = requests.get(f"{NIFI_API_BASE}/controller-services/{cs_id}").json()
    version = detail["revision"]["version"]

    payload = {"revision": {"version": version}, "state": "ENABLED"}
    resp = requests.put(
        f"{NIFI_API_BASE}/controller-services/{cs_id}/run-status", json=payload
    )
    resp.raise_for_status()
    print(f"[+] Controller Service {cs_id} 已啟動啟用")


def configure_and_start_put_database_record(
    processor_id: str, dbcp_id: str, reader_id: str
):
    """設定 PutDatabaseRecord 必要屬性並啟動處理器"""
    proc_detail = requests.get(
        f"{NIFI_API_BASE}/processors/{processor_id}"
    ).json()
    version = proc_detail["revision"]["version"]

    # 補足 4 個缺失的屬性，並設定 autoTerminated 關係避免狀態無效
    config_payload = {
        "revision": {"version": version},
        "component": {
            "id": processor_id,
            "config": {
                "properties": {
                    "Record Reader": reader_id,
                    "Database Connection Pooling Service": dbcp_id,
                    "Statement Type": DB_CONFIG["statement_type"],
                    "Table Name": DB_CONFIG["table_name"],
                },
                "autoTerminatedRelationships": ["success", "failure", "retry"],
            },
        },
    }

    update_resp = requests.put(
        f"{NIFI_API_BASE}/processors/{processor_id}", json=config_payload
    )
    update_resp.raise_for_status()
    print(f"[+] 處理器 {processor_id} 屬性更新完成，驗證錯誤已清除")

    # 取得最新版本號以啟動處理器
    updated_proc = update_resp.json()
    latest_version = updated_proc["revision"]["version"]

    # 啟動處理器
    start_payload = {"revision": {"version": latest_version}, "state": "RUNNING"}
    start_resp = requests.put(
        f"{NIFI_API_BASE}/processors/{processor_id}/run-status",
        json=start_payload,
    )
    start_resp.raise_for_status()
    print(f"[+] 處理器 {processor_id} 已成功運行 (RUNNING)，佇列開始消化")


def main():
    print("=== 開始修正 NiFi Process Group [Local_2_SQL] ===")

    # 1. 建立並啟用 DBCPConnectionPool
    dbcp_id = create_or_get_dbcp_service(PROCESS_GROUP_ID)

    # 2. 建立並啟用 Record Reader (依資料格式選 'json' 或 'csv')
    reader_id = create_or_get_record_reader(PROCESS_GROUP_ID, reader_type="json")

    # 稍候服務在 NiFi 內部生效
    time.sleep(2)

    # 3. 修正 PutDatabaseRecord 缺失屬性並轉為 RUNNING
    configure_and_start_put_database_record(
        PUT_DB_PROCESSOR_ID, dbcp_id, reader_id
    )

    print("=== 所有錯誤修復完成 ===")


if __name__ == "__main__":
    main()