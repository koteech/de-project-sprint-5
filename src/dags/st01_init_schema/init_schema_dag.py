import logging
import os

import pendulum
from airflow.decorators import dag, task
from st01_init_schema.schema_init import SchemaDdl
from lib import PgConnect

log = logging.getLogger(__name__)

PG_WAREHOUSE_CONNECTION = {
    "host": "localhost",
    "user": "jovyan",
    "password": "jovyan",
    "port": 5432,
    "ssl": False,
    "database": "de"
}

# Путь до папки с SQL-скриптами
DDL_FILES_PATH = "/lessons/dags/st01_init_schema/ddl"


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['de_project_sprint5', 'schema', 'ddl'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def st01_01_de_project_sprint5_init_schema_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = PgConnect(
        host=PG_WAREHOUSE_CONNECTION["host"],
        port=PG_WAREHOUSE_CONNECTION["port"],
        db_name=PG_WAREHOUSE_CONNECTION["database"],
        user=PG_WAREHOUSE_CONNECTION["user"],
        pw=PG_WAREHOUSE_CONNECTION["password"],
        sslmode="require" if PG_WAREHOUSE_CONNECTION["ssl"] else "disable"
    )

    ddl_path = DDL_FILES_PATH

    @task(task_id="init")
    def init():
        rest_loader = SchemaDdl(dwh_pg_connect, log)
        rest_loader.init_schema(os.path.join(ddl_path))

    # Определение порядка выполнения тасков
    init()


# Вызываем функцию, описывающую даг.
init_dag = st01_01_de_project_sprint5_init_schema_dag()  # noqa
