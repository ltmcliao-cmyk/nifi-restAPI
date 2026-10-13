# pgs/flowfile_generator.py
import nipyapi

GENERATOR_PG_NAME = "FlowFile_Generator"


def create_flowfile_generator_pg(parent_pg=None, **kwargs):
    if parent_pg is None:
        root_id = nipyapi.canvas.get_root_pg_id()
        parent_pg = nipyapi.canvas.get_process_group(
            root_id, identifier_type="id"
        )

    def get_type(name):
        t = nipyapi.canvas.get_processor_type(name)
        return t[0] if isinstance(t, list) else t

    # 1. 建立獨立的子 Process Group (確保 generator_pg.component.name 與 routes 正常)
    existing_pgs = nipyapi.canvas.list_all_process_groups(parent_pg.id)
    gen_pg = next(
        (pg for pg in existing_pgs if pg.component.name == GENERATOR_PG_NAME),
        None,
    )
    if not gen_pg:
        gen_pg = nipyapi.canvas.create_process_group(
            parent_pg=parent_pg,
            new_pg_name=GENERATOR_PG_NAME,
            location=(300, 300),
        )

    # 2. 建立狂噴資料的 Generator (每 0.2 秒一顆)
    gen = nipyapi.canvas.create_processor(
        parent_pg=gen_pg,
        processor=get_type("GenerateFlowFile"),
        location=(100, 100),
        name="Gen_Continuous_Data",
    )
    nipyapi.canvas.update_processor(
        gen,
        nipyapi.nifi.ProcessorConfigDTO(
            scheduling_period="0.2 sec", properties={"File Size": "10KB"}
        ),
    )

    # 3. 建立 Sink (故意停止以堆積佇列)
    sink = nipyapi.canvas.create_processor(
        parent_pg=gen_pg,
        processor=get_type("LogAttribute"),
        location=(100, 350),
        name="Queue_Sink_Stopped",
    )
    nipyapi.canvas.update_processor(
        sink,
        nipyapi.nifi.ProcessorConfigDTO(
            auto_terminated_relationships=["success"]
        ),
    )

    # 4. 建立連線
    nipyapi.canvas.create_connection(
        source=gen,
        target=sink,
        relationships=["success"],
        name="Pressure_Queue",
    )

    # 5. 故意放一個無效的處理器 (讓儀表板出現 Invalid Processors 警示)
    nipyapi.canvas.create_processor(
        parent_pg=gen_pg,
        processor=get_type("UpdateAttribute"),
        location=(400, 100),
        name="Invalid_Test_Processor",
    )

    # 6. 先啟動產生器，並確保 Sink 保持停止
    nipyapi.canvas.schedule_processor(gen, True)
    nipyapi.canvas.schedule_processor(sink, False)

    return gen_pg