import logging

import pendulum
from airflow.decorators import dag, task
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
    schedule_interval='0 0 10 * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['de_project_sprint5', 'courier_ledger', 'cdm'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def st04_01_de_project_sprint5_courier_ledger_dds_to_cdm_dag():
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
    @task(task_id="courier_ledger_load")
    def load_courier_ledger():
        sql_query = """
                        insert into cdm.dm_courier_ledger    
                        select t3.*
                        from (
                            select t2.*,
                                courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
                            from
                                (select t1.*,
                                    case
                                        when t1.rate_avg < 4 then case when t1.orders_total_sum * 0.05 < 100 then 100 else t1.orders_total_sum * 0.05 end
                                        when t1.rate_avg >= 4 and t1.rate_avg < 4.5 then case when t1.orders_total_sum * 0.07 < 150 then 150 else t1.orders_total_sum * 0.07 end
                                        when t1.rate_avg >= 4.5 and t1.rate_avg < 4.9 then case when t1.orders_total_sum * 0.08 < 175 then 175 else t1.orders_total_sum * 0.08 end
                                        when t1.rate_avg > 4.9 then case when t1.orders_total_sum * 0.1 < 200 then 200 else t1.orders_total_sum * 0.1 end
                                    end as courier_order_sum

                                from
                                    (select
                                        d.courier_id,
                                        c.courier_name as courier_name,
                                        extract(year from d.delivery_ts)::INT as settlement_year,
                                        extract(month from d.delivery_ts)::INT as settlement_month,
                                        count(d.order_id)::INT as orders_count,
                                        sum(d.sum) as orders_total_sum,
                                        avg(d.rate) as rate_avg,
                                        sum(d.sum)*0.25 as order_processing_fee,
                                        sum(tip_sum) as courier_tips_sum

                                    from dds.dm_deliveries d
                                    inner join dds.dm_couriers c on d.courier_id=c.courier_id
                                    group by d.courier_id, c.courier_name, extract(year from d.delivery_ts), extract(month from d.delivery_ts))

                                    as t1)
                                as t2
                            ) as t3

                            where settlement_year=extract(  year from now())::INT and settlement_month=extract(month from now())::INT
                """

        # Выполнить SQL-запрос.
        with dwh_pg_connect.cursor() as cursor:
            cursor.execute(sql_query)

        # Закрыть подключение.
        dwh_pg_connect.close()

    # Инициализируем объявленные таски.
    courier_ledger_init = load_courier_ledger()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    courier_ledger_init  # type: ignore


    init_dag = st04_01_de_project_sprint5_courier_ledger_dds_to_cdm_dag()  # noqa
