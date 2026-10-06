#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
腳本名稱：fetch_all_nifi_context.py
功能說明：
    直接連線本機/容器的 Apache NiFi 1.12.1 REST API (http://localhost:8080/nifi-api)，
    遞迴抓取系統資源狀態、Bulletins 告警、Controller Services、
    所有 Process Groups、Processors 與 Connection 隊列積壓數據，
    並直接輸出為格式化 JSON 檔案。
"""

import os
import sys
import json
import logging
from typing import Dict, Any, List, Optional
import requests
import urllib3

# 關閉 SSL 不安全連線警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定標準記錄輸出格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ==============================================================================
# 連線設定（明文設定區）
# ==============================================================================
# 依據 compose.md，NiFi 1.12.1 對外開放 8080 端口，走純 HTTP 協議
NIFI_API_URL = "http://localhost:8080/nifi-api"

# 若後續有啟用安全驗證機制，可在此直接填入帳密；預設 1.12.1 HTTP 模式設為 None 即可
NIFI_USERNAME: Optional[str] = None
NIFI_PASSWORD: Optional[str] = None

# 是否校驗 SSL 憑證（HTTP 模式下無影響）
VERIFY_SSL = False

# JSON 輸出檔名
OUTPUT_JSON_PATH = "nifi_context.json"
# ==============================================================================


class NiFiContextFetcher:
    """NiFi REST API 資料收集器"""

    def __init__(self, base_url: str, username: Optional[str] = None, password: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.verify = VERIFY_SSL
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

    def authenticate(self) -> None:
        """若有設定帳號密碼，則請求 JWT Token；無帳密則直接略過"""
        if not self.username or not self.password:
            logger.info("未指定帳密，以純 HTTP 免認證模式請求 NiFi API。")
            return

        token_url = f"{self.base_url}/access/token"
        logger.info("正在請求身分驗證 Token: %s", token_url)
        try:
            resp = self.session.post(
                token_url,
                data={"username": self.username, "password": self.password},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=15
            )
            resp.raise_for_status()
            token = resp.text
            self.session.headers["Authorization"] = f"Bearer {token}"
            logger.info("Token 取得成功，已載入請求標頭。")
        except requests.RequestException as e:
            logger.error("NiFi 登入失敗: %s", e)
            sys.exit(1)

    def _get(self, endpoint: str) -> Dict[str, Any]:
        """封裝 GET 請求與例外防呆"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error("請求失敗 [%s]: %s", url, e)
            return {}

    def get_system_diagnostics(self) -> Dict[str, Any]:
        """抓取 NiFi 系統層級 JVM、記憶體、硬碟使用量與執行緒狀態"""
        logger.info("正在獲取系統硬體與 JVM 診斷數據...")
        data = self._get("/system-diagnostics")
        return data.get("systemDiagnostics", {})

    def get_bulletins(self) -> List[Dict[str, Any]]:
        """抓取 Flow 的 Bulletins（系統警報與錯誤事件日誌）"""
        logger.info("正在獲取系統層級 Bulletins 警報...")
        data = self._get("/flow/bulletin-board")
        board = data.get("bulletinBoard", {})
        return board.get("bulletins", [])

    def get_controller_services(self) -> List[Dict[str, Any]]:
        """抓取根層級的所有 Controller Services（例如 DBCPConnectionPool 等共用連線池）"""
        logger.info("正在獲取 Controller Services 狀態...")
        data = self._get("/flow/process-groups/root/controller-services")
        services = []
        for cs in data.get("controllerServices", []):
            comp = cs.get("component", {})
            services.append({
                "id": comp.get("id"),
                "name": comp.get("name"),
                "type": comp.get("type"),
                "state": comp.get("state"),
                "validation_errors": comp.get("validationErrors", [])
            })
        return services

    def fetch_process_group_tree(self, pg_id: str = "root") -> Dict[str, Any]:
        """
        遞迴遍歷 Process Group 樹狀架構，深入每一層抓取：
        1. Processors：執行狀態、讀寫位元組量、FlowFiles 吞吐量
        2. Connections：佇列數量（Queued Count）、積壓大小與背壓門檻
        3. 子群組（Sub Process Groups）
        """
        logger.info("正在分析 Process Group: %s", pg_id)
        flow_data = self._get(f"/flow/process-groups/{pg_id}")
        process_group_flow = flow_data.get("processGroupFlow", {})
        flow = process_group_flow.get("flow", {})

        group_node = {
            "id": process_group_flow.get("id"),
            "breadcrumb": process_group_flow.get("breadcrumb", {}).get("breadcrumb", {}).get("name", "root"),
            "processors": [],
            "connections": [],
            "child_process_groups": []
        }

        # 1. 整理 Processors
        for proc in flow.get("processors", []):
            comp = proc.get("component", {})
            status = proc.get("status", {})
            agg = status.get("aggregateSnapshot", {})
            group_node["processors"].append({
                "id": comp.get("id"),
                "name": comp.get("name"),
                "type": comp.get("type"),
                "state": comp.get("state"),
                "validation_errors": comp.get("validationErrors", []),
                "run_status": agg.get("runStatus"),
                "bytes_read": agg.get("bytesRead"),
                "bytes_written": agg.get("bytesWritten"),
                "flowfiles_in": agg.get("flowFilesIn"),
                "flowfiles_out": agg.get("flowFilesOut")
            })

        # 2. 整理 Connections (佇列積壓與背壓狀態)
        for conn in flow.get("connections", []):
            comp = conn.get("component", {})
            status = conn.get("status", {})
            agg = status.get("aggregateSnapshot", {})
            group_node["connections"].append({
                "id": comp.get("id"),
                "name": comp.get("name") or f"{comp.get('source', {}).get('name')} -> {comp.get('destination', {}).get('name')}",
                "source_id": comp.get("source", {}).get("id"),
                "source_name": comp.get("source", {}).get("name"),
                "destination_id": comp.get("destination", {}).get("id"),
                "destination_name": comp.get("destination", {}).get("name"),
                "queued_count": agg.get("queuedCount"),
                "queued_size": agg.get("queuedSize"),
                "percent_use_count": agg.get("percentUseCount"),
                "percent_use_bytes": agg.get("percentUseBytes"),
                "backpressure_object_threshold": comp.get("backPressureObjectThreshold"),
                "backpressure_data_size_threshold": comp.get("backPressureDataSizeThreshold")
            })

        # 3. 遞迴抓取子群組
        for child_pg in flow.get("processGroups", []):
            child_id = child_pg.get("id")
            if child_id:
                child_tree = self.fetch_process_group_tree(child_id)
                group_node["child_process_groups"].append(child_tree)

        return group_node

    def run(self) -> Dict[str, Any]:
        """執行全量上下文收集並回傳字典結構"""
        self.authenticate()
        
        full_context = {
            "metadata": {
                "nifi_api_url": self.base_url,
                "version_hint": "Apache NiFi 1.12.1"
            },
            "system_diagnostics": self.get_system_diagnostics(),
            "bulletins": self.get_bulletins(),
            "controller_services": self.get_controller_services(),
            "flow_hierarchy": self.fetch_process_group_tree("root")
        }
        return full_context


def main():
    logger.info("啟動 NiFi 狀態收集程序，目標位址：%s", NIFI_API_URL)

    fetcher = NiFiContextFetcher(
        base_url=NIFI_API_URL,
        username=NIFI_USERNAME,
        password=NIFI_PASSWORD
    )

    data = fetcher.run()

    # 輸出至 JSON 檔案（2 格縮排，避免中文字元轉譯為 unicode escape）
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info("抓取完成！檔案已儲存至：%s", os.path.abspath(OUTPUT_JSON_PATH))


if __name__ == "__main__":
    main()