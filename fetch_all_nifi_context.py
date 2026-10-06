# -*- coding: utf-8 -*-
import requests
import json

NIFI_BASE_URL = "http://localhost:8080/nifi-api"

def get_process_group_recursive(session, pg_id="root"):
    """
    遞迴走訪 Process Group 拓樸樹：
    GET /process-groups/{id} 端點會回傳該 PG 內部的 Processors、Connections 與子 PG (processGroups) [1]。
    """
    res = session.get(f"{NIFI_BASE_URL}/process-groups/{pg_id}").json()
    component = res.get("component", {})
    contents = component.get("contents", {})

    # 1. 提取目前 PG 內部的 Processors [1]
    processors = [
        {
            "id": p.get("id"),
            "name": p.get("name"),
            "type": p.get("type"),
            "state": p.get("component", {}).get("state")
        }
        for p in contents.get("processors", [])
    ]

    # 2. 提取目前 PG 內部的 Connections [1]
    connections = [
        {
            "id": c.get("id"),
            "name": c.get("name"),
            "source": c.get("source", {}).get("name"),
            "destination": c.get("destination", {}).get("name")
        }
        for c in contents.get("connections", [])
    ]

    # 3. 遞迴走訪子 Process Groups [1]
    child_pgs = []
    for child in contents.get("processGroups", []):
        child_id = child.get("id")
        # 遞迴呼叫取得子 PG 結構
        child_pgs.append(get_process_group_recursive(session, child_id))

    return {
        "id": component.get("id"),
        "name": component.get("name"),
        "processors": processors,
        "connections": connections,
        "childProcessGroups": child_pgs
    }

def collect_all_connections(pg_tree):
    """
    從 PG 樹狀結構中，輔助收集全系統所有的 connection_id 供 Queue 檢查使用 [1]
    """
    conn_list = list(pg_tree.get("connections", []))
    for child in pg_tree.get("childProcessGroups", []):
        conn_list.extend(collect_all_connections(child))
    return conn_list

def generate_universal_diagnostic_json():
    session = requests.Session()

    # 1. 拓樸結構端點 (GET /process-groups/{id}) - 遞迴取得全系統 PG 藍圖 [1]
    topology_tree = get_process_group_recursive(session, "root")

    # 2. 即時效能端點 (GET /flow/process-groups/root/status?recursive=true) [1]
    # 使用 root + recursive=true 可以一次取得包含全系統所有子 PG 的即時效能與積壓數據 [1]
    res_status = session.get(f"{NIFI_BASE_URL}/flow/process-groups/root/status?recursive=true").json()
    agg_snapshot = res_status.get("processGroupStatus", {}).get("aggregateSnapshot", {})

    performance_status = {
        "flowFilesQueued": agg_snapshot.get("flowFilesQueued"),
        "bytesQueued": agg_snapshot.get("bytesQueued"),
        "activeThreadCount": agg_snapshot.get("activeThreadCount"),
        "processorSnapshots": [
            {
                "id": ps.get("id"),
                "name": ps.get("name"),
                "runStatus": ps.get("runStatus"),
                "activeThreadCount": ps.get("activeThreadCount"),
                "flowFilesIn": ps.get("flowFilesIn"),
                "flowFilesOut": ps.get("flowFilesOut")
            }
            for ps in agg_snapshot.get("processorStatusSnapshots", [])
        ]
    }

    # 3. 錯誤警告端點 (GET /flow/bulletin-board) [1]
    res_bulletins = session.get(f"{NIFI_BASE_URL}/flow/bulletin-board").json()
    bulletins = [
        {
            "id": b.get("id"),
            "timestamp": b.get("timestamp"),
            "sourceId": b.get("sourceId"),
            "level": b.get("level"),
            "message": b.get("message")
        }
        for b in res_bulletins.get("bulletinBoard", {}).get("bulletins", [])
    ]

    # 4. 佇列排錯端點 (GET /flowfile-queues/{connection_id}/listing) [1]
    # 針對全系統每一個 Connection 檢查是否有積壓 FlowFile [1]
    all_connections = collect_all_connections(topology_tree)
    queue_listings = {}

    for conn in all_connections:
        conn_id = conn["id"]
        res_queue = session.get(f"{NIFI_BASE_URL}/flowfile-queues/{conn_id}/listing").json()
        summaries = res_queue.get("listingRequest", {}).get("flowFileSummaries", [])
        if summaries:
            queue_listings[conn_id] = [
                {
                    "uuid": ff.get("uuid"),
                    "position": ff.get("position"),
                    "size": ff.get("size"),
                    "queuedDuration": ff.get("queuedDuration")
                }
                for ff in summaries
            ]

    # 打包成單一通用 AI 上下文報告
    return {
        "topologyTree": topology_tree,
        "performanceStatus": performance_status,
        "bulletins": bulletins,
        "queueListings": queue_listings
    }

if __name__ == "__main__":
    full_report = generate_universal_diagnostic_json()
    with open("nifi_full_ai_context.json", "w", encoding="utf-8") as f:
        json.dump(full_report, f, ensure_ascii=False, indent=2)
    print("已成功匯出全系統 Process Group 診斷檔：nifi_full_ai_context.json")