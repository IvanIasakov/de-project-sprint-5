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

class OrderPrObj(BaseModel):
    id: int
    xid_product: str
    xid_order: str
    bonus_paymet: float
    bonus_grant: float
    
class OrderObjStg(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class OrderOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, user_threshold: int, limit: int) -> List[OrderPrObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObjStg)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM stg.bonussystem_events
                    WHERE id > %(threshold)s and event_type='bonus_transaction' --Пропускаем те объекты, которые уже загрузили.
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
                 el=str2json(record.event_value)
                 for itm in el['product_payments']:
                     ell=str2json(str(itm).replace("'","\""))
                     objs.append(OrderPrObj(**{'id':record.id,'xid_product':ell['product_id'], 'xid_order':el['order_id'], 'bonus_paymet':ell['bonus_payment'],'bonus_grant':ell['bonus_grant']}))
            
            
            #objs=[str2json(record.object_value) for record in objsin]
            #objs=objsin
        return objs


class OrderDestRepository:
    
    def insert_order(self, conn: Connection, order: OrderPrObj) -> None:
      
        with conn.cursor() as cur:
            cur.execute(
                """
                    UPDATE dds.fct_product_sales SET bonus_payment=%(bonus_paymet)s, bonus_grant=%(bonus_grant)s
                    WHERE product_id=(SELECT DISTINCT ID FROM dds.dm_products WHERE product_id=%(xid_product)s) and order_id=(SELECT DISTINCT ID FROM dds.dm_orders WHERE order_key=%(xid_order)s);
                """,
                {
                    "xid_product": order.xid_product,
                    "xid_order": order.xid_order,
                    "bonus_paymet": order.bonus_paymet,
                    "bonus_grant": order.bonus_grant
                },
            )


class BonusLoader:
    WF_KEY = "bonussys_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 30000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrderOriginRepository(pg_origin)
        self.stg = OrderDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_bonus(self):
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
