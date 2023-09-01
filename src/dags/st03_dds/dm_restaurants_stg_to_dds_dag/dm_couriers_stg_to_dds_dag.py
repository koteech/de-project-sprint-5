import logging

import pendulum
from airflow.decorators import dag, task
from st03_dds.dm_restaurants_stg_to_dds_dag.events_loader import restaurantLoader
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


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['de_project_sprint5', 'restaurants', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def st03_04_de_project_sprint5_restaurants_stg_to_dds_dag():
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
    @task(task_id="restaurants_load")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = restaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    restaurants_dict = load_restaurants()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    restaurants_dict  # type: ignore


init_dag = st03_04_de_project_sprint5_restaurants_stg_to_dds_dag()  # noqa
