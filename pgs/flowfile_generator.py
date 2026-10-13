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

    # 3. 建立下游 Sink (保持 STOPPED 以累積佇列)
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
    conn = nipyapi.canvas.create_connection(
        source=gen,
        target=sink,
        relationships=["success"],
        name="Pressure_Queue",
    )

    # 5. 設定 30 秒過期機制 (滿了自動丟棄舊資料，永遠流動且絕不塞爆硬碟)
    conn_entity = nipyapi.nifi.ConnectionsApi().get_connection(conn.id)
    conn_entity.component.flow_file_expiration = "30 sec"
    nipyapi.nifi.ConnectionsApi().update_connection(
        id=conn.id,
        body=conn_entity,
    )

    # 6. 放一個無效處理器供儀表板呈現警告
    nipyapi.canvas.create_processor(
        parent_pg=gen_pg,
        processor=get_type("UpdateAttribute"),
        location=(400, 100),
        name="Invalid_Test_Processor",
    )

    # 7. 啟動產生器 (下游 Sink 維持停止)
    nipyapi.canvas.schedule_processor(gen, True)

    return gen_pg