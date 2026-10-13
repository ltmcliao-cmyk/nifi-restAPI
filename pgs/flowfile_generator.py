# pgs/flowfile_generator.py
import nipyapi

GENERATOR_PG_NAME = "FlowFile_Generator"

def create_flowfile_generator_pg(parent_pg=None, **kwargs):
    if parent_pg is None:
        parent_pg = nipyapi.canvas.get_process_group(nipyapi.canvas.get_root_pg_id(), "id")

    def get_type(name):
        t = nipyapi.canvas.get_processor_type(name)
        return t[0] if isinstance(t, list) else t

    # 建立獨立 Process Group
    gen_pg = nipyapi.canvas.create_process_group(
        parent_pg=parent_pg,
        new_pg_name=GENERATOR_PG_NAME,
        location=(300, 300),
    )

    # 內部產生器
    gen = nipyapi.canvas.create_processor(
        gen_pg, get_type("GenerateFlowFile"), (100, 100), "Gen_Continuous_Data"
    )
    nipyapi.canvas.update_processor(
        gen,
        nipyapi.nifi.ProcessorConfigDTO(
            scheduling_period="0.5 sec", properties={"File Size": "100B"}
        ),
    )

    # 內部 Sink (停止狀態)
    sink = nipyapi.canvas.create_processor(
        gen_pg, get_type("LogAttribute"), (100, 350), "Queue_Sink_Stopped"
    )
    nipyapi.canvas.update_processor(
        sink,
        nipyapi.nifi.ProcessorConfigDTO(auto_terminated_relationships=["success"]),
    )

    # 內部連線
    conn = nipyapi.canvas.create_connection(
        source=gen, target=sink, relationships=["success"], name="Pressure_Queue"
    )
    nipyapi.canvas.update_connection(
        conn,
        nipyapi.nifi.ConnectionDTO(
            back_pressure_object_threshold=500,
            back_pressure_data_size_threshold="10 MB",
        ),
    )

    # 內部放一個無效 Processor 給儀表板偵測警告
    nipyapi.canvas.create_processor(
        gen_pg, get_type("UpdateAttribute"), (400, 100), "Invalid_Test_Processor"
    )

    return gen_pg