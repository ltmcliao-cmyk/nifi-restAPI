import nipyapi

# 1. 取得根畫布
root = nipyapi.canvas.get_process_group(nipyapi.canvas.get_root_pg_id(), 'id')

def get_type(name):
    t = nipyapi.canvas.get_processor_type(name)
    return t[0] if isinstance(t, list) else t

# 2. 建立 Generator (每 0.2 秒噴一顆，點亮執行緒與 FlowFile 總數)
gen = nipyapi.canvas.create_processor(
    root, get_type('GenerateFlowFile'), (300, 100), 'Gen_Alert'
)
nipyapi.canvas.update_processor(
    gen, nipyapi.nifi.ProcessorConfigDTO(
        scheduling_period='0.2 sec',
        properties={'File Size': '10KB', 'Custom Text': '${now()}'}
    )
)

# 3. 建立 LogMessage (bulletin_level 設為 INFO，觸發即時 Bulletin 告警通知)
log_proc = nipyapi.canvas.create_processor(
    root, get_type('LogMessage'), (300, 350), 'Trigger_Bulletin'
)
nipyapi.canvas.update_processor(
    log_proc, nipyapi.nifi.ProcessorConfigDTO(
        bulletin_level='INFO',
        properties={'log-level': 'warn', 'log-message': '系統壓力測試中：FlowFile 正在持續湧入！'}
    )
)

# 4. 建立下游 Sink (維持 STOPPED，堆積佇列)
sink = nipyapi.canvas.create_processor(
    root, get_type('LogAttribute'), (300, 600), 'Blocked_Sink'
)
nipyapi.canvas.update_processor(
    sink, nipyapi.nifi.ProcessorConfigDTO(auto_terminated_relationships=['success'])
)

# 5. 建立故意損壞的處理器 (無連線，點亮「無效處理器 Invalid Processors」)
nipyapi.canvas.create_processor(
    root, get_type('UpdateAttribute'), (600, 100), 'Invalid_Test_Proc'
)

# 6. 連線並將背壓設為 50 (點亮「壅塞/佇列積壓連線」)
conn1 = nipyapi.canvas.create_connection(gen, log_proc, ['success'], 'Active_Feed')
conn2 = nipyapi.canvas.create_connection(log_proc, sink, ['success'], 'Congested_Queue')
nipyapi.canvas.update_connection(
    conn2, nipyapi.nifi.ConnectionDTO(back_pressure_object_threshold=50)
)

# 7. 啟動產生器與日誌處理器
nipyapi.canvas.schedule_processor(gen, True)
nipyapi.canvas.schedule_processor(log_proc, True)

print(">>> 部署完成！")