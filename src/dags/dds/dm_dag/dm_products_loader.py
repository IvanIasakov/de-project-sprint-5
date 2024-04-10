from logging import Logger
from typing import List

from dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime

class ProductObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_xid: str

    

class RestObjStg(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class ProdsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, user_threshold: int, limit: int) -> List[ProductObj]:
        with self._db.client().cursor(row_factory=class_row(RestObjStg)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
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
                 for mi in el['menu']:
                     ell=str2json(str(mi).replace("'","\""))
                     objs.append(ProductObj(**{'id':record.id,'product_id':ell['_id'],'product_name':ell['name'], 'product_price':ell['price'], 'active_from':el['update_ts'],'active_to':datetime(year=2099,month=12,day=31), 'restaurant_xid':el['_id']}))
            
            
            #objs=[str2json(record.object_value) for record in objsin]
            #objs=objsin
        return objs


class ProdsDestRepository:
    ACT_DATE = datetime(year=2099,month=12,day=31)
    def insert_product(self, conn: Connection, product: ProductObj) -> None:
      
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price,active_from, active_to)
                    VALUES ((SELECT DISTINCT ID FROM dds.dm_restaurants WHERE restaurant_id=%(restaurant_id)s),%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET restaurant_id=EXCLUDED.restaurant_id, product_id=EXCLUDED.product_id, product_name = EXCLUDED.product_name, product_price = EXCLUDED.product_price, active_from = EXCLUDED.active_from;
                """,
                {
                    "restaurant_id": product.restaurant_xid,
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": product.active_to
                },
            )


class ProductLoader:
    WF_KEY = "products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 30000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProdsOriginRepository(pg_origin)
        self.stg = ProdsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):
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
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} rests to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for prod in load_queue:
                self.stg.insert_product(conn, prod)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
