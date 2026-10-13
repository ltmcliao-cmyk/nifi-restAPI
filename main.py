# main.py
import os
import nipyapi
from pgs import local_2_SQL, flowfile_generator
import routes

# 移除：import monitor_publisher


def main():
  nifi_host = os.getenv("NIFI_HOST", "http://localhost:8080")
  nipyapi.config.nifi_config.host = f"{nifi_host.rstrip('/')}/nifi-api"

  root_id = nipyapi.canvas.get_root_pg_id()
  root_pg = nipyapi.canvas.get_process_group(root_id, identifier_type="id")

  db_config = {
      "url": "jdbc:postgresql://postgres:5432/pipeline_db",
      "driver_class": "org.postgresql.Driver",
      "driver_location": (
          "/opt/nifi/nifi-current/drivers/postgresql-driver"
      ),
      "user": "postgres",
      "password": "postgrespassword123",
  }

  raw_data_dir = "/opt/nifi/nifi-current/data/raw"

  # 建立拓撲
  local_pg = local_2_SQL.create_local_2_sql_pg(
      parent_pg=root_pg,
      db_config=db_config,
      table_name="raw_bike_availability",
      input_dir=raw_data_dir,
      file_filter=".*",
  )

  generator_pg = flowfile_generator.create_flowfile_generator_pg(
      parent_pg=root_pg
  )

  if hasattr(routes, "setup_inter_pg_routes"):
    routes.setup_inter_pg_routes(local_pg=local_pg, generator_pg=generator_pg)
  elif hasattr(routes, "create_inter_pg_routes"):
    routes.create_inter_pg_routes(local_pg=local_pg, generator_pg=generator_pg)

  # 啟動拓撲
  nipyapi.canvas.schedule_process_group(local_pg.id, scheduled=True)
  nipyapi.canvas.schedule_process_group(generator_pg.id, scheduled=True)

  print(
      "[OK] Successfully deployed Process Groups:"
      f" '{local_pg.component.name}', '{generator_pg.component.name}'"
  )
  # 到此乾淨結束，不要去叫 monitor_publisher！


if __name__ == "__main__":
  main()