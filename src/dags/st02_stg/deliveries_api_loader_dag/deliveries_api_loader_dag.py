import logging

import pendulum
from airflow.decorators import dag, task
from lib import PgConnect
from st02_stg.deliveries_api_loader_dag.events_loader import deliveryLoader

log = logging.getLogger(__name__)

PG_WAREHOUSE_CONNECTION = {
    "host": "localhost",
    "user": "jovyan",
    "password": "jovyan",
    "port": 5432,
    "ssl": False,
    "database": "de"
}


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['de_project_sprint5', 'deliveries', 'load'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def st02_02_de_project_sprint5_deliveries_api_loader_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = PgConnect(
        host=PG_WAREHOUSE_CONNECTION["host"],
        port=PG_WAREHOUSE_CONNECTION["port"],
        db_name=PG_WAREHOUSE_CONNECTION["database"],
        user=PG_WAREHOUSE_CONNECTION["user"],
        pw=PG_WAREHOUSE_CONNECTION["password"],
        sslmode="require" if PG_WAREHOUSE_CONNECTION["ssl"] else "disable"
    )

    # Объявляем таск, который загружает данные.
    @task(task_id="deliveries_load")
    def load_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = deliveryLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    load_deliveries_init = load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    load_deliveries_init  # type: ignore


init_dag = st02_02_de_project_sprint5_deliveries_api_loader_dag()  # noqa
