# -*- coding: utf-8 -*-
"""
routes.py - 跨 Process Group (Port-to-Port) 拓樸定義模組
設計邏輯與精神：
1. 第一性原理：routes.py 專責處理跨邊界（PG 之間 via InputPort / OutputPort）的資料流拓樸。
2. 奧卡姆剃刀：目前系統僅有單一 Process Group (local_2_SQL)，內部 Processor 拓樸已各自內聚於 PG 內，
   因此本模組僅保留高階介面，無須進行多餘的內部連線轉載。
"""

import nipyapi

def build_inter_pg_routes():
    """
    建立跨 Process Group 之間的 Port-to-Port 連線拓樸。
    目前為單一 Process Group 架構，預留此介面供未來的拓樸鏈結擴充。
    """
    pass