# -*- coding: utf-8 -*-
"""
routes.py - 跨 Process Group (Port-to-Port / Boundary) 拓樸定義模組
設計邏輯與精神：
1. 第一性原理：routes.py 專責處理跨邊界（PG 之間 via InputPort / OutputPort 或外層 Source -> InputPort）的連線拓樸。
2. 奧卡姆剃刀：群組內部處理器連線各自內聚於 PG；跨群組邊界連線統一集中於此。
"""

import nipyapi


def connect_to_input_port(source_component, target_input_port, relationships=None, name=None):
    """
    建立從外部元件（Processor 或 OutputPort）連接至子群組 Input Port 的跨邊界連線。
    
    Args:
        source_component: 來源組件（Processor 或 Output Port 實例）
        target_input_port: 目標 Process Group 的 Input Port 實例
        relationships (list, optional): 若來源為 Processor 則需指定關聯（如 ['success']）
        name (str, optional): 連線名稱
    """
    kwargs = {
        'source': source_component,
        'target': target_input_port
    }
    if relationships:
        kwargs['relationships'] = relationships
    if name:
        kwargs['name'] = name

    return nipyapi.canvas.create_connection(**kwargs)


def build_inter_pg_routes(source_pg_ports=None, target_pg_ports=None):
    """
    建立跨 Process Group 之間的 Port-to-Port 連線拓樸。
    目前為單一 Process Group，預留介面供後續多群組鏈結使用。
    """
    # 範例：若有多個 PG 時：
    # if source_pg_ports and target_pg_ports:
    #     nipyapi.canvas.create_connection(
    #         source=source_pg_ports['output_port'],
    #         target=target_pg_ports['input_port']
    #     )
    pass