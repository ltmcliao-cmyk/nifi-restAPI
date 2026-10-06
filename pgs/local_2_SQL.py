#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import requests

NIFI_API_BASE = "http://127.0.0.1:8080/nifi-api"
DEFAULT_PG_ID = "10d7800a-01a1-1000-e2d4-9b50ad6cf816"
PUT_DB_PROCESSOR_ID = "10d78562-01a1-1000-adec-5b289ce54b88"

# 資料庫連線配置
DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/your_database",
    "driver_class": "org.postgresql.Driver",
    "driver_location": "/opt/nifi/drivers/postgresql-42.2.14.jar",  # 請確認容器或本機路徑
    "user": "postgres",
    "password": "your_password",
    "table_name": "target_table",
    "statement_type": "INSERT",
}


def get_pg_info(pg_id: str, nifi_url: str = NIFI_API_BASE):
    """取得 Process Group 資訊與版本號"""
    resp = requests.get(f"{nifi_url}/process-groups/{pg_id}")
    resp.raise_for_status()
    return resp.json()


def create_or_get_dbcp_service(pg_id: str, nifi_url: str = NIFI_API_BASE) -> str:
    """在指定 Process Group 建立並啟用 PostgreSQL DBCPConnectionPool"""
    url = f"{nifi_url}/process-groups/{pg_id}/controller-services"

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
        services = requests.get(
            f"{nifi_url}/flow/process-groups/{pg_id}/controller-services"
        ).json()
        cs_id = None
        for svc in services.get("controllerServices", []):
            if "DBCPConnectionPool" in svc["component"]["type"]:
                cs_id = svc["id"]
                break
        if not cs_id:
            resp.raise_for_status()

    enable_controller_service(cs_id, nifi_url)
    return cs_id


def create_or_get_record_reader(
    pg_id: str, reader_type: str = "json", nifi_url: str = NIFI_API_BASE
) -> str:
    """建立並啟用 Record Reader (JsonTreeReader 或 CSVReader)"""
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

    url = f"{nifi_url}/process-groups/{pg_id}/controller-services"
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
            f"{nifi_url}/flow/process-groups/{pg_id}/controller-services"
        ).json()
        cs_id = None
        for svc in services.get("controllerServices", []):
            if reader_type in svc["component"]["type"].lower():
                cs_id = svc["id"]
                break
        if not cs_id:
            resp.raise_for_status()

    enable_controller_service(cs_id, nifi_url)
    return cs_id


def enable_controller_service(cs_id: str, nifi_url: str = NIFI_API_BASE):
    """啟用指定 Controller Service"""
    detail = requests.get(f"{nifi_url}/controller-services/{cs_id}").json()
    version = detail["revision"]["version"]
    current_state = detail["component"]["state"]

    if current_state == "ENABLED":
        return

    payload = {"revision": {"version": version}, "state": "ENABLED"}
    resp = requests.put(
        f"{nifi_url}/controller-services/{cs_id}/run-status", json=payload
    )
    resp.raise_for_status()
    print(f"[+] Controller Service {cs_id} 已啟動啟用")


def configure_and_start_put_database_record(
    processor_id: str,
    dbcp_id: str,
    reader_id: str,
    nifi_url: str = NIFI_API_BASE,
):
    """設定 PutDatabaseRecord 必要屬性並啟動處理器"""
    proc_detail = requests.get(f"{nifi_url}/processors/{processor_id}").json()
    version = proc_detail["revision"]["version"]

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
        f"{nifi_url}/processors/{processor_id}", json=config_payload
    )
    update_resp.raise_for_status()
    print(f"[+] 處理器 {processor_id} 屬性更新完成")

    latest_version = update_resp.json()["revision"]["version"]
    start_payload = {"revision": {"version": latest_version}, "state": "RUNNING"}
    start_resp = requests.put(
        f"{nifi_url}/processors/{processor_id}/run-status",
        json=start_payload,
    )
    start_resp.raise_for_status()
    print(f"[+] 處理器 {processor_id} 狀態已變更為 RUNNING")


def create_local_2_sql_pg(
    parent_pg_id: str = "root",
    nifi_url: str = NIFI_API_BASE,
    pg_id: str = DEFAULT_PG_ID,
    **kwargs,
):
    """
    提供給 main.py 呼叫的入口函式。
    取得/建立 Local_2_SQL Process Group，修復並啟動相依服務與處理器。
    """
    print(f"[*] 正在初始化/配置 Local_2_SQL Process Group (ID: {pg_id})...")

    # 1. 取得 Process Group 資訊（若不存在可拋出異常或透過 parent_pg_id 建立）
    pg_info = get_pg_info(pg_id, nifi_url=nifi_url)

    # 2. 建立並啟用 Controller Services
    dbcp_id = create_or_get_dbcp_service(pg_id, nifi_url=nifi_url)
    reader_id = create_or_get_record_reader(
        pg_id, reader_type="json", nifi_url=nifi_url
    )

    time.sleep(1)

    # 3. 修復並啟動 PutDatabaseRecord
    configure_and_start_put_database_record(
        PUT_DB_PROCESSOR_ID, dbcp_id, reader_id, nifi_url=nifi_url
    )

    print("[*] Local_2_SQL Process Group 配置完成")
    return pg_info


if __name__ == "__main__":
    create_local_2_sql_pg()