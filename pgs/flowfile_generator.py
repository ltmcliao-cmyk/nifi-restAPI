import nipyapi

root = nipyapi.canvas.get_process_group(nipyapi.canvas.get_root_pg_id(), 'id')

def get_type(name):
    t = nipyapi.canvas.get_processor_type(name)
    return t[0] if isinstance(t, list) else t

# 1. 產生器 (每 0.2 秒狂噴)
gen = nipyapi.canvas.create_processor(root, get_type('GenerateFlowFile'), (300, 100), 'Gen_Data')

# 2. 下游 Sink (保持停止，讓 Queue 一直堆)
sink = nipyapi.canvas.create_processor(root, get_type('LogAttribute'), (300, 300), 'Stop_Sink')
nipyapi.canvas.update_processor(sink, nipyapi.nifi.ProcessorConfigDTO(auto_terminated_relationships=['success']))

# 3. 故意放一個壞掉的 Processor (讓網頁顯示 Invalid / 錯誤警告)
nipyapi.canvas.create_processor(root, get_type('UpdateAttribute'), (600, 100), 'Invalid_Proc')

# 4. 接線並啟動產生器
nipyapi.canvas.create_connection(gen, sink, ['success'], 'Pressure_Queue')
nipyapi.canvas.schedule_processor(gen, True)

print(">>> 搞定！Queue 與資料已經開始跑了！")