# pgs/flowfile_generator.py
import time
import nipyapi

GENERATOR_PG_NAME = "FlowFile_Generator"
GENERATE_PROC_NAME = "Generate Continuous Test Data"
SINK_PROC_NAME = "Queue Sink (Stopped)"


def log(msg, level="INFO"):
  print(f"[DEPLOY-LOG {time.strftime('%H:%M:%S')}] [{level}] {msg}", flush=True)


def create_flowfile_generator_pg(parent_pg=None, **kwargs):
  """建立並配置持續產生 FlowFile 並堆積在 Queue 的測試用 Process Group"""
  log("=== [START] FlowFile Generator 流程圖配置部署 ===", "STEP")

  if parent_pg is None:
    root_id = nipyapi.canvas.get_root_pg_id()
    parent_pg = nipyapi.canvas.get_process_group(root_id, identifier_type="id")

  # 1. 檢查或建立 Process Group
  existing_pgs = nipyapi.canvas.list_all_process_groups(parent_pg.id)
  gen_pg = next(
      (pg for pg in existing_pgs if pg.component.name == GENERATOR_PG_NAME),
      None,
  )

  if not gen_pg:
    log(f"建立新的 Process Group: {GENERATOR_PG_NAME}", "INFO")
    gen_pg = nipyapi.canvas.create_process_group(
        parent_pg=parent_pg,
        new_pg_name=GENERATOR_PG_NAME,
        location=(300, 300),
    )
  log(
      f"鎖定目標 Process Group: {gen_pg.component.name} (UUID: {gen_pg.id})",
      "OK",
  )

  procs = nipyapi.canvas.list_all_processors(gen_pg.id)

  # 2. 建立或取得 GenerateFlowFile 處理器
  gen_proc = next(
      (p for p in procs if p.component.name == GENERATE_PROC_NAME), None
  )
  if not gen_proc:
    log("建立 GenerateFlowFile 處理器...", "INFO")
    proc_type = nipyapi.canvas.get_processor_type("GenerateFlowFile")
    if isinstance(proc_type, list):
      proc_type = proc_type[0]

    gen_proc = nipyapi.canvas.create_processor(
        parent_pg=gen_pg,
        processor=proc_type,
        location=(100, 100),
        name=GENERATE_PROC_NAME,
    )

  # 3. 建立或取得下游 Sink 處理器（故意保持 STOPPED 讓 Queue 積壓）
  sink_proc = next(
      (p for p in procs if p.component.name == SINK_PROC_NAME), None
  )
  if not sink_proc:
    log("建立下游 Sink 處理器（用於累積 Queue）...", "INFO")
    proc_type = nipyapi.canvas.get_processor_type("LogAttribute")
    if isinstance(proc_type, list):
      proc_type = proc_type[0]

    sink_proc = nipyapi.canvas.create_processor(
        parent_pg=gen_pg,
        processor=proc_type,
        location=(100, 300),
        name=SINK_PROC_NAME,
    )
    # 標記 success 為 auto-terminate 確保驗證合法 (VALID)
    nipyapi.canvas.update_processor(
        sink_proc,
        nipyapi.nifi.ProcessorConfigDTO(
            auto_terminated_relationships=["success"]
        ),
    )

  # 4. 建立 Connection（如果尚未連線）
  conns = nipyapi.canvas.list_all_connections(gen_pg.id)
  target_conn = next(
      (
          c
          for c in conns
          if c.source_id == gen_proc.id and c.destination_id == sink_proc.id
      ),
      None,
  )
  if not target_conn:
    log("建立 GenerateFlowFile -> Sink 連線佇列...", "INFO")
    nipyapi.canvas.create_connection(
        source=gen_proc,
        target=sink_proc,
        relationships=["success"],
        name="Test Pressure Queue",
    )

  # 5. 配置 GenerateFlowFile（更新前先確保停止，避免 RUNNING 衝突）
  # 重新取得最新實例狀態以確保 state 同步
  gen_proc = nipyapi.canvas.get_processor(gen_proc.id, identifier_type="id")
  if gen_proc.component.state == "RUNNING":
    log("暫停 GenerateFlowFile 處理器以更新配置...", "INFO")
    nipyapi.canvas.schedule_processor(gen_proc, False)
    # 等待狀態確實轉為 STOPPED
    gen_proc = nipyapi.canvas.get_processor(gen_proc.id, identifier_type="id")

  test_payload = '{"status": "monitoring", "timestamp": "${now():toNumber()}"}'
  config_props = {
      "File Size": "1B",
      "Batch Size": "1",
      "Data Format": "Text",
      "Custom Text": test_payload,
  }

  nipyapi.canvas.update_processor(
      gen_proc,
      nipyapi.nifi.ProcessorConfigDTO(
          properties=config_props,
          scheduling_period="1 sec",
          auto_terminated_relationships=[],  # 走 success 連線，不直接銷毀
      ),
  )

  # 啟動產生器（下游維持停止）
  nipyapi.canvas.schedule_processor(gen_proc, True)

  log("=== [FINISH] FlowFile Generator 部署完成（佇列開始積壓） ===", "OK")
  return gen_pg