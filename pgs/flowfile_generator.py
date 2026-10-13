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

    # 1. 建立獨立的 Process Group
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

    # 2. 建立資料產生器 (每 0.2 秒狂噴)
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

    # 3. 建立下游 Sink (保持停止，讓 Queue 持續積壓)
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

    # 4. 接線
    nipyapi.canvas.create_connection(
        source=gen,
        target=sink,
        relationships=["success"],
        name="Pressure_Queue",
    )

    # 5. 放一個無效處理器 (讓儀表板抓到 Invalid Processors)
    nipyapi.canvas.create_processor(
        parent_pg=gen_pg,
        processor=get_type("UpdateAttribute"),
        location=(400, 100),
        name="Invalid_Test_Processor",
    )

    return gen_pg