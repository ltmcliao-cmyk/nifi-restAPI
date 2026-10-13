# pgs 資料夾下的 flowfile_generator 模組
import time
import nipyapi

# 定義目標 Process Group 與處理器名稱
GENERATOR_PG_NAME = "FlowFile_Generator"
GENERATE_PROC_NAME = "Generate Continuous Test Data"

def log(msg, level="INFO"):
    print(f"[DEPLOY-LOG {time.strftime('%H:%M:%S')}] [{level}] {msg}", flush=True)

def create_flowfile_generator_pg(parent_pg=None, **kwargs):
    """建立並配置持續產生 FlowFile 的測試用 Process Group"""
    log("=== [START] FlowFile Generator 流程圖配置部署 ===", "STEP")
    
    if parent_pg is None:
        root_id = nipyapi.canvas.get_root_pg_id()
        parent_pg = nipyapi.canvas.get_process_group(root_id, identifier_type="id")

    # 1. 檢查或建立 Process Group
    existing_pgs = nipyapi.canvas.list_all_process_groups(parent_pg.id)
    gen_pg = next((pg for pg in existing_pgs if pg.component.name == GENERATOR_PG_NAME), None)
    
    if not gen_pg:
        log(f"建立新的 Process Group: {GENERATOR_PG_NAME}", "INFO")
        gen_pg = nipyapi.canvas.create_process_group(
            parent_pg=parent_pg,
            new_pg_name=GENERATOR_PG_NAME,
            location=(300, 300)
        )
    log(f"鎖定目標 Process Group: {gen_pg.component.name} (UUID: {gen_pg.id})", "OK")

    # 2. 檢查或建立 GenerateFlowFile 處理器
    procs = nipyapi.canvas.list_all_processors(gen_pg.id)
    gen_proc = next((p for p in procs if p.component.name == GENERATE_PROC_NAME), None)

    if not gen_proc:
        log("建立 GenerateFlowFile 處理器...", "INFO")
        proc_type = nipyapi.canvas.get_processor_type("GenerateFlowFile")
        if isinstance(proc_type, list):
            proc_type = proc_type[0]
            
        gen_proc = nipyapi.canvas.create_processor(
            parent_pg=gen_pg,
            processor=proc_type,
            location=(100, 100),
            name=GENERATE_PROC_NAME
        )

    # 3. 配置 GenerateFlowFile 屬性 (每秒自動產生 1 筆測試 FlowFile)
    test_payload = '{"status": "monitoring", "timestamp": "${now():toNumber()}"}'
    config_props = {
        "File Size": "1B",
        "Batch Size": "1",
        "Data Format": "Text",
        "Custom Text": test_payload
    }

    nipyapi.canvas.update_processor(
        gen_proc,
        nipyapi.nifi.ProcessorConfigDTO(
            properties=config_props,
            scheduling_period="1 sec",
            auto_terminated_relationships=["success"]
        )
    )
    log(f"GenerateFlowFile 處理器配置完成 (UUID: {gen_proc.id})", "OK")

    log("=== [FINISH] FlowFile Generator 部署完成 ===", "OK")
    return gen_pg