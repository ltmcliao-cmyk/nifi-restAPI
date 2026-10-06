#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
腳本名稱：fetch_all_nifi_context.py
適用環境：Windows X64 Self-hosted Runner + 本地 Docker Apache NiFi 1.12.1
功能說明：
    1. 探活本機 NiFi REST API (http://127.0.0.1:8080/nifi-api)
    2. 遞迴抓取 Process Groups、Processors、Connections (隊列積壓)、Bulletins 與 Controller Services
    3. 輸出 nifi_context.json
    4. 自動將 JSON 內容渲染至 GitHub Actions 執行總覽頁面 (GITHUB_STEP_SUMMARY)，可直接一鍵複製
"""

import os
import sys
import json
import logging
from typing import Dict, Any, List, Optional
import requests
import urllib3

# 關閉 SSL 不安全連線警告（HTTP 模式下備用）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定 Logging 格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ==============================================================================
# 明文連線配置區
# ==============================================================================
# 建議優先使用 127.0.0.1，避免 Windows 將 localhost 解析為 IPv6 [::1] 導致連線失敗
NIFI_API_URL = "http://127.0.0.1:8080/nifi-api"

# NiFi 1.12.1 預設 HTTP 8080 無需帳密認證，設為 None 即可；若有啟用驗證請在此填寫
NIFI_USERNAME: Optional[str] = None
NIFI_PASSWORD: Optional[str] = None

# 本地輸出 JSON 檔名
OUTPUT_JSON_PATH = "nifi_context.json"
# ==============================================================================


class NiFiContextFetcher:
    """NiFi REST API 抓取與結構化處理類別"""

    def __init__(self, base_url: str, username: Optional[str] = None, password: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

    def check_connection(self) -> None:
        """
        健康探活：確認本地 NiFi 服務是否在線。
        連線失敗時直接中斷，避免產生空 JSON 誤導維運。
        """
        test_url = f"{self.base_url}/flow/about"
        logger.info("正在探活 NiFi 連線：%s ...", test_url)
        try:
            resp = self.session.get(test_url, timeout=10)
            resp.raise_for_status()
            about_data = resp.json().get("about", {})
            logger.info("✅ 成功連線至 Apache NiFi！版本資訊：%s (建置時間: %s)",
                        about_data.get("version", "未知"),
                        about_data.get("buildTimestamp", "未知"))
        except requests.exceptions.ConnectionError as err:
            logger.error("❌ 無法連線至 NiFi API [%s]！", self.base_url)
            logger.error("請確認本機 Docker 中的 nifi container 是否正在運行中 (docker compose ps)。")
            logger.error("詳細錯誤: %s", err)
            sys.exit(1)
        except requests.RequestException as err:
            logger.error("❌ 探活失敗，HTTP 請求錯誤：%s", err)
            sys.exit(1)

    def authenticate(self) -> None:
        """若有提供帳密，則換取 JWT Token（純 HTTP 匿名模式自動略過）"""
        if not self.username or not self.password:
            logger.info("未提供帳密，以純 HTTP 匿名模式操作 NiFi API。")
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
            self.session.headers["Authorization"] = f"Bearer {resp.text}"
            logger.info("Token 取得成功，已載入 Session Headers。")
        except requests.RequestException as e:
            logger.error("NiFi 登入失敗: %s", e)
            sys.exit(1)

    def _get(self, endpoint: str) -> Dict[str, Any]:
        """通用的 GET 請求封裝"""
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning("請求端點失敗 [%s]: %s", url, e)
            return {}

    def get_system_diagnostics(self) -> Dict[str, Any]:
        """抓取 NiFi 系統層級 JVM、記憶體、硬碟使用量與線程數據"""
        logger.info("獲取 JVM 與系統資源狀態...")
        data = self._get("/system-diagnostics")
        return data.get("systemDiagnostics", {})

    def get_bulletins(self) -> List[Dict[str, Any]]:
        """抓取系統層級的最新告警與錯誤訊息 (Bulletins)"""
        logger.info("獲取系統 Bulletins 警報...")
        data = self._get("/flow/bulletin-board")
        return data.get("bulletinBoard", {}).get("bulletins", [])

    def get_controller_services(self) -> List[Dict[str, Any]]:
        """抓取根層級的所有 Controller Services (連線池、SSL 等)"""
        logger.info("獲取根層級 Controller Services...")
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
        遞迴抓取指定 Process Group 及其底下所有元件狀態
        :param pg_id: Process Group ID，預設為 "root"
        """
        logger.info("遍歷 Process Group 拓撲: %s", pg_id)
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

        # 1. 抽取 Processors
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

        # 2. 抽取 Connections (隊列積壓、FlowFile 數量與背壓狀態)
        for conn in flow.get("connections", []):
            comp = conn.get("component", {})
            status = conn.get("status", {})
            agg = status.get("aggregateSnapshot", {})
            group_node["connections"].append({
                "id": comp.get("id"),
                "name": comp.get("name") or f"{comp.get('source', {}).get('name')} -> {comp.get('destination', {}).get('name')}",
                "source_name": comp.get("source", {}).get("name"),
                "destination_name": comp.get("destination", {}).get("name"),
                "queued_count": agg.get("queuedCount"),
                "queued_size": agg.get("queuedSize"),
                "percent_use_count": agg.get("percentUseCount"),
                "percent_use_bytes": agg.get("percentUseBytes"),
                "backpressure_object_threshold": comp.get("backPressureObjectThreshold"),
                "backpressure_data_size_threshold": comp.get("backPressureDataSizeThreshold")
            })

        # 3. 遞迴子 Process Groups
        for child_pg in flow.get("processGroups", []):
            child_id = child_pg.get("id") or child_pg.get("component", {}).get("id")
            if child_id:
                child_tree = self.fetch_process_group_tree(child_id)
                group_node["child_process_groups"].append(child_tree)

        return group_node

    def collect(self) -> Dict[str, Any]:
        """執行探活、認證並整合完整數據字典"""
        self.check_connection()
        self.authenticate()

        return {
            "metadata": {
                "nifi_api_url": self.base_url,
                "engine": "Apache NiFi 1.12.1"
            },
            "system_diagnostics": self.get_system_diagnostics(),
            "bulletins": self.get_bulletins(),
            "controller_services": self.get_controller_services(),
            "flow_hierarchy": self.fetch_process_group_tree("root")
        }


def write_github_summary(json_content_str: str) -> None:
    """
    將 JSON 直接寫入 GitHub Actions Step Summary。
    由 Python 原生處理檔案與 UTF-8 編碼，完美跨平臺，避免 Windows PowerShell 的轉譯衝突。
    """
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if not summary_path:
        logger.info("未檢測到 GITHUB_STEP_SUMMARY 環境變數（非 CI 環境執行），略過摘要寫入。")
        return

    file_size_bytes = len(json_content_str.encode("utf-8"))
    logger.info("正在寫入 GitHub Action 執行頁面摘要 (大小: %d bytes)...", file_size_bytes)

    try:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("### 📊 NiFi 即時運行狀態 (JSON)\n\n")
            f.write(f"- **連線端點**: `{NIFI_API_URL}`\n")
            f.write(f"- **產物檔案**: `{OUTPUT_JSON_PATH}` ({file_size_bytes} bytes)\n\n")

            # GitHub Summary 限制單頁最大約 1024KB，若未超標則直接嵌入高亮代碼區塊
            if file_size_bytes < 900 * 1024:
                f.write("<details open><summary><b>點擊展開 / 複製完整 JSON</b></summary>\n\n")
                f.write("```json\n")
                f.write(json_content_str)
                f.write("\n```\n")
                f.write("</details>\n")
            else:
                f.write("> ⚠️ **提示**：JSON 檔案過大 (>900KB)，為確保頁面流暢，請直接下載本執行頁面下方的 Artifact 檔案。\n")
        logger.info("✅ GitHub Action 頁面摘要寫入完成！")
    except Exception as e:
        logger.warning("寫入 GITHUB_STEP_SUMMARY 失敗: %s", e)


def main():
    logger.info("開始抓取 NiFi 狀態，目標: %s", NIFI_API_URL)

    fetcher = NiFiContextFetcher(
        base_url=NIFI_API_URL,
        username=NIFI_USERNAME,
        password=NIFI_PASSWORD
    )

    data = fetcher.collect()

    # 序列化為漂亮排版的 JSON 字串
    json_str = json.dumps(data, ensure_ascii=False, indent=2)

    # 1. 寫入本地實體檔案
    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        f.write(json_str)
    logger.info("✅ JSON 檔案已儲存至: %s", os.path.abspath(OUTPUT_JSON_PATH))

    # 2. 直接寫入 GitHub Actions 網頁 Summary
    write_github_summary(json_str)


if __name__ == "__main__":
    main()