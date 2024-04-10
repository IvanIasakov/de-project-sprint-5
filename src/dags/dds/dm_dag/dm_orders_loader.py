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

class OrderObj(BaseModel):
    id: int
    xid_user: str
    xid_restaurant: str
    order_date: datetime
    order_key: str
    order_status: str

class OrderObjStg(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class OrderOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, user_threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObjStg)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objsin = cur.fetchall()
            objs = []
            for record in objsin:
                 el=str2json(record.object_value)
                 objs.append(OrderObj(**{'id':record.id,'xid_user':str2json(str(el['user']).replace("'","\""))['id'], 'xid_restaurant':str2json(str(el['restaurant']).replace("'","\""))['id'],'order_date':el['date'],'order_key':el['_id'],'order_status':el['final_status']}))
            
            
            #objs=[str2json(record.object_value) for record in objsin]
            #objs=objsin
        return objs


class OrderDestRepository:
    
    def insert_order(self, conn: Connection, order: OrderObj) -> None:
      
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
                    VALUES ((SELECT DISTINCT ID FROM dds.dm_users WHERE user_id=%(xid_user)s),(SELECT DISTINCT ID FROM dds.dm_restaurants WHERE restaurant_id=%(xid_restaurants)s),(SELECT DISTINCT ID FROM dds.dm_timestamps WHERE ts=%(order_date)s),%(order_key)s, %(order_status)s)
                    ON CONFLICT (id) DO UPDATE
                    SET user_id=EXCLUDED.user_id,restaurant_id = EXCLUDED.restaurant_id, timestamp_id = EXCLUDED.timestamp_id, order_key = EXCLUDED.order_key, order_status = EXCLUDED.order_status;
                """,
                {
                    "xid_user": order.xid_user,
                    "xid_restaurants": order.xid_restaurant,
                    "order_date": order.order_date,
                    "order_key": order.order_key,
                    "order_status": order.order_status
                },
            )


class OrdersLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 30000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrderOriginRepository(pg_origin)
        self.stg = OrderDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
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
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.stg.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
