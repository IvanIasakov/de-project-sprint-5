from logging import Logger
from typing import List

from dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime
from datetime import time

class DeliveryObj(BaseModel):
    id: int
    xid_courier: str
    xid_order: str
    delivery_key: str
    delivery_ts: datetime
    adress: str
    rate: int
    tip_sum: float
    total_sum: float
    update_ts: datetime



class DeliveryOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliverys(self, user_threshold: datetime) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT id,  
                    ((object_value::jsonb)->>'courier_id')::varchar as xid_courier,
                    ((object_value::jsonb)->>'order_id')::varchar as xid_order,
                    ((object_value::jsonb)->>'delivery_id')::varchar as delivery_key,
                    ((object_value::jsonb)->>'delivery_ts')::timestamp as delivery_ts,
                    ((object_value::jsonb)->>'address')::varchar as adress,
                    ((object_value::jsonb)->'rate')::int4 as rate,
                    ((object_value::jsonb)->'tip_sum')::numeric(14,2) as tip_sum,
                    ((object_value::jsonb)->'sum')::numeric(14,2) as total_sum
                    ,update_ts
                    FROM stg.deliverysystem_deliverys
                    WHERE update_ts > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY update_ts ASC; --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                """, {
                    "threshold": user_threshold
                }
            )
            objs = cur.fetchall()
            
        return objs


class DeliveryDestRepository:
    
    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
      
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliverys(courier_id, order_id, delivery_key, delivery_ts, address, rate, tip_sum, total_sum)
                    VALUES ((SELECT DISTINCT ID FROM dds.dm_couriers WHERE courier_id=%(xid_courier)s),(SELECT DISTINCT ID FROM dds.dm_orders WHERE order_key=%(xid_orders)s),%(delivery_key)s,%(delivery_ts)s, %(adress)s, %(rate)s ,%(tip_sum)s, %(total_sum)s)
                    ON CONFLICT (delivery_key) DO UPDATE
                    SET courier_id=EXCLUDED.courier_id,order_id = EXCLUDED.order_id, delivery_ts = EXCLUDED.delivery_ts, address = EXCLUDED.address, rate = EXCLUDED.rate, tip_sum=EXCLUDED.tip_sum, total_sum=EXCLUDED.total_sum;
                """,
                {
                    "xid_courier": delivery.xid_courier,
                    "xid_orders": delivery.xid_order,
                    "delivery_key": delivery.delivery_key,
                    "delivery_ts": delivery.delivery_ts,
                    "adress": delivery.adress,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum,
                    "total_sum": delivery.total_sum
                },
            )


class DeliverysLoader:
    WF_KEY = "deliverys_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ts"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveryOriginRepository(pg_origin)
        self.stg = DeliveryDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliverys(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: datetime(2022, 1, 1).isoformat()})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliverys(last_loaded)
            self.log.info(f"Found {len(load_queue)} deliverys to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.stg.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.update_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
