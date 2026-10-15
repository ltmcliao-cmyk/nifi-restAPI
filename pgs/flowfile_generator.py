# pgs/flowfile_generator.py
import nipyapi

GENERATOR_PG_NAME = "FlowFile_Generator"


def create_flowfile_generator_pg(parent_pg=None, **kwargs):
    if parent_pg is None:
        parent_pg = nipyapi.canvas.get_process_group(
            nipyapi.canvas.get_root_pg_id(), "id"
        )

    def get_type(name):
        t = nipyapi.canvas.get_processor_type(name)
        return t[0] if isinstance(t, list) else t

    # 1. 建立獨立 Process Group
    gen_pg = nipyapi.canvas.create_process_group(
        parent_pg=parent_pg,
        new_pg_name=GENERATOR_PG_NAME,
        location=(300, 300),
    )

    # 2. 建立資料產生器 (每 0.5 秒產生 100B 小資料)
    gen = nipyapi.canvas.create_processor(
        parent_pg=gen_pg,
        processor=get_type("GenerateFlowFile"),
        location=(100, 100),
        name="Gen_Continuous_Data",
    )
    nipyapi.canvas.update_processor(
        gen,
        nipyapi.nifi.ProcessorConfigDTO(
            scheduling_period="0.5 sec",
            properties={"File Size": "100B"},
        ),
    )

    # 3. 建立下游 Sink：重點！設定 1 秒消化一次，並開啟 auto-terminate
    sink = nipyapi.canvas.create_processor(
        parent_pg=gen_pg,
        processor=get_type("LogAttribute"),
        location=(100, 350),
        name="Queue_Sink_Flowing",
    )
    nipyapi.canvas.update_processor(
        sink,
        nipyapi.nifi.ProcessorConfigDTO(
            scheduling_period="0 sec",  # 慢速消化，確保 Queue 保持積壓又一直在動
            auto_terminated_relationships=["success"],
        ),
    )

    # 4. 建立連線
    nipyapi.canvas.create_connection(
        source=gen,
        target=sink,
        relationships=["success"],
        name="Pressure_Queue",
    )

    # 5. 放一個無效處理器供儀表板呈現警告
    nipyapi.canvas.create_processor(
        parent_pg=gen_pg,
        processor=get_type("UpdateAttribute"),
        location=(400, 100),
        name="Invalid_Test_Processor",
    )

    # 6. 同時啟動兩者（資料開始持續流動！）
    nipyapi.canvas.schedule_processor(gen, True)
    nipyapi.canvas.schedule_processor(sink, True)

    return gen_pg