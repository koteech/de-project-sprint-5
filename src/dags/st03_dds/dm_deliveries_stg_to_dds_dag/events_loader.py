from datetime import datetime
from logging import Logger
from typing import List

from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from st03_dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository


class deliveryObj(BaseModel):
    id: int
    order_id: str
    order_ts: datetime
    delivery_id: str
    courier_id: str
    address: str
    delivery_ts: datetime
    rate: int
    sum: float
    tip_sum: float


class deliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, delivery_threshold: int, limit: int) -> List[deliveryObj]:
        with self._db.client().cursor(row_factory=class_row(deliveryObj)) as cur:
            cur.execute(
                """
                    SELECT t1.*
                    FROM (select distinct de.id,
                                          (de.object_value ::JSON ->> 'order_id')    AS order_id,
                                          (de.object_value ::JSON ->> 'order_ts')    AS order_ts,
                                          (de.object_value ::JSON ->> 'delivery_id') AS delivery_id,
                                          (de.object_value ::JSON ->> 'courier_id')  AS courier_id,
                                          (de.object_value ::JSON ->> 'address')     AS address,
                                          (de.object_value ::JSON ->> 'delivery_ts') AS delivery_ts,
                                          (de.object_value ::JSON ->> 'rate')        AS rate,
                                          (de.object_value ::JSON ->> 'sum')         AS sum,
                                          (de.object_value ::JSON ->> 'tip_sum')     AS tip_sum
                          from stg.stg_deliveries de
                          ) as t1
                          
                    WHERE t1.id > %(threshold)s
                    ORDER BY t1.id ASC
                    LIMIT %(limit)s;
                """,
                {
                    "threshold": delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()

        return objs


class deliveriesDestRepository:

    def insert_delivery(self, conn: Connection, delivery: deliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(id, order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
                    VALUES (%(id)s, %(order_id)s, %(order_ts)s, %(delivery_id)s, %(courier_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(sum)s, %(tip_sum)s);
                """,
                {
                    "id": delivery.id,
                    "order_id": delivery.order_id,
                    "order_ts": delivery.order_ts,
                    "delivery_id": delivery.delivery_id,
                    "courier_id": delivery.courier_id,
                    "address": delivery.address,
                    "delivery_ts": delivery.delivery_ts,
                    "rate": delivery.rate,
                    "sum": delivery.sum,
                    "tip_sum": delivery.tip_sum
                },
            )


class deliveryLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = deliveriesOriginRepository(pg_origin)
        self.dds = deliveriesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.dds.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
