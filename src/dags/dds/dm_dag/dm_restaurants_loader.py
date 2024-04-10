from logging import Logger
from typing import List

from dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime

class RestObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime

    

class RestObjStg(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class RestsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_rests(self, user_threshold: int, limit: int) -> List[RestObj]:
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
                 objs.append(RestObj(**{'id':record.id,'restaurant_id':el['_id'],'restaurant_name':el['name'],'active_from':el['update_ts'],'active_to':datetime(year=2099,month=12,day=31)}))
            
            
            #objs=[str2json(record.object_value) for record in objsin]
            #objs=objsin
        return objs


class RestDestRepository:
    ACT_DATE = datetime(year=2099,month=12,day=31)
    def insert_rest(self, conn: Connection, rest: RestObj) -> None:
      
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET restaurant_id=EXCLUDED.restaurant_id,restaurant_name = EXCLUDED.restaurant_name, active_from = EXCLUDED.active_from;
                """,
                {
                    "restaurant_id": rest.restaurant_id,
                    "restaurant_name": rest.restaurant_name,
                    "active_from": rest.active_from,
                    "active_to": rest.active_to
                },
            )


class RestLoader:
    WF_KEY = "rests_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 30000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = RestsOriginRepository(pg_origin)
        self.stg = RestDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_rests(self):
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
            load_queue = self.origin.list_rests(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} rests to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for rest in load_queue:
                self.stg.insert_rest(conn, rest)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
