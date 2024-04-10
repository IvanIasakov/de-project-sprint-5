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

class TsObj(BaseModel):
    id: int
    ts: datetime

class OrderObjStg(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class TsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_ts(self, user_threshold: int, limit: int) -> List[TsObj]:
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
                 objs.append(TsObj(**{'id':record.id,'ts':el['date']}))
            
            
            #objs=[str2json(record.object_value) for record in objsin]
            #objs=objsin
        return objs


class TsDestRepository:
    
    def insert_ts(self, conn: Connection, rts: TsObj) -> None:
      
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (id) DO UPDATE
                    SET ts=EXCLUDED.ts,year = EXCLUDED.year, month = EXCLUDED.month, day = EXCLUDED.day, time = EXCLUDED.time, date = EXCLUDED.date;
                """,
                {
                    "ts": rts.ts,
                    "year": rts.ts.year,
                    "month": rts.ts.month,
                    "day": rts.ts.day,
                    "time": rts.ts.time(),
                    "date": rts.ts.date()
                },
            )


class TsLoader:
    WF_KEY = "timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 30000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TsOriginRepository(pg_origin)
        self.stg = TsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_tss(self):
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
            load_queue = self.origin.list_ts(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} rests to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for rest in load_queue:
                self.stg.insert_ts(conn, rest)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
