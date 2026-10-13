import nipyapi

# 1. 取得根畫布
root = nipyapi.canvas.get_process_group(nipyapi.canvas.get_root_pg_id(), 'id')

# 2. 建立 Generator (每 0.2 秒狂噴，點亮「執行緒」與「FlowFile 總數」)
gen = nipyapi.canvas.create_processor(
    root, nipyapi.canvas.get_processor_type('GenerateFlowFile')[0], (300, 100), 'Gen_Alert'
)
nipyapi.canvas.update_processor(
    gen, nipyapi.nifi.ProcessorConfigDTO(
        scheduling_period='0.2 sec',
        properties={'File Size': '10KB', 'Custom Text': '${now()}'}
    )
)

# 3. 建立故意報錯的 LogMessage (直接觸發「即時 Bulletin 告警」)
# bulletin_level='INFO' 代表只要一跑就會把訊息直接丟進系統布告欄
log_proc = nipyapi.canvas.create_processor(
    root, nipyapi.canvas.get_processor_type('LogMessage')[0], (300, 350), 'Trigger_Bulletin'
)
nipyapi.canvas.update_processor(
    log_proc, nipyapi.nifi.ProcessorConfigDTO(
        bulletin_level='INFO',
        properties={'log-level': 'warn', 'log-message': '系統壓力測試中：FlowFile 正在持續湧入！'}
    )
)

# 4. 建立故意停用下游的 Sink (累積「壅塞佇列」)
sink = nipyapi.canvas.create_processor(
    root, nipyapi.canvas.get_processor_type('LogAttribute')[0], (300, 600), 'Blocked_Sink'
)
nipyapi.canvas.update_processor(sink, nipyapi.nifi.ProcessorConfigDTO(auto_terminated_relationships=['success']))

# 5. 建立故意損壞的處理器 (點亮「無效處理器 Invalid Processors」)
# 不設定 relationship 也不 auto-terminate，它會保持 INVALID 狀態
nipyapi.canvas.create_processor(
    root, nipyapi.canvas.get_processor_type('UpdateAttribute')[0], (600, 100), 'Invalid_Test_Proc'
)

# 6. 連線並限制容量 (故意設背壓 50，讓儀表板的「壅塞/佇列積壓連線」爆滿變紅)
conn1 = nipyapi.canvas.create_connection(gen, log_proc, ['success'], 'Active_Feed')
conn2 = nipyapi.canvas.create_connection(log_proc, sink, ['success'], 'Congested_Queue')
# 將連線背壓設為 50，立刻觸發容量使用率與積壓警示
nipyapi.canvas.update_connection(conn2, nipyapi.nifi.ConnectionDTO(back_pressure_object_threshold=50))

# 7. 一鍵啟動 (下游維持 STOPPED，讓中繼站開始積壓與狂發 Bulletin)
nipyapi.canvas.schedule_processor(gen, True)
nipyapi.canvas.schedule_processor(log_proc, True)

print(">>> 部署完成！請刷新網頁看數據、積壓紅字與 Bulletin 警告跳動！")