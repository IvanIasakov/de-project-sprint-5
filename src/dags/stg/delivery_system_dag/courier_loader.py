from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable
from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime
import requests
import time


class CourierObj(BaseModel):
    id: int
    id_courier: str
    object_value: str 
    update_ts: datetime



class CourierOriginRepository:
    def __init__(self,log: Logger) -> None:
         self.log=log

    def list_couriers(self, limit: int) -> List[CourierObj]:
                api_conn = BaseHook.get_connection('DELIVERY_SYSTEM_API')
                x_api_key = Variable.get('DELIVERY_SYSTEM_X-API-KEY')
                x_nickname= Variable.get('DELIVERY_SYSTEM_X-Nickname')
                x_cohotrt = Variable.get('DELIVERY_SYSTEM_X-Cohort')
                X_HEADERS={"X-API-KEY": x_api_key,
                           "X-Nickname": x_nickname,
                           "X-Cohort": x_cohotrt}
                api_endpoint = api_conn.host
                method_url = '/couriers'
                api_url='https://'+ api_endpoint + method_url
                OObj=[]
                offset=0
                while True:
                    x_params = {'sort_field': "_id"
                          ,'sort_direction': "asc"
                          ,'limit': limit
                          ,'offset': offset}
                    offset+=limit
                    
                    session = requests.Session()
                    for i in range(5):
                        try:
                            response = session.get(api_url, headers=X_HEADERS,params=x_params)
                            response.raise_for_status()
                        except requests.exceptions.ConnectionError as err:
                            self.log.error(err)
                            time.sleep(10)
                        if response.status_code == 200:
                            list_response = list(response.json())
                            self.log.info('Recieved part load objects: %s' , len(list_response))
                            break
                        elif i == 4:
                            raise TimeoutError("TimeoutError fail to get deliveries.")
                    for el in list_response:
                         OObj.append(CourierObj(**{'id':0,'id_courier':el['_id'],'object_value':str(el).replace("'","\""),'update_ts':datetime.now()}))
               
                    if len(list_response)<limit:
                         break                
                return OObj


class CourierDestRepository:

    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(id_courier,object_value,update_ts)
                    VALUES (%(id_courier)s, %(object_value)s, %(update_ts)s)
                    ON CONFLICT (id_courier) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,update_ts=EXCLUDED.update_ts;
                """,
                {
                    "id_courier": courier.id_courier,
                    "object_value": courier.object_value,
                    "update_ts": courier.update_ts
                },
            )


class CourierLoader:
    WF_KEY = "delivery_couriers_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ts"
    BATCH_LIMIT = 50  # Читаем по кусочками из API - размер кусочка.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierOriginRepository(log)
        self.stg = CourierDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
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
            #last_date = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            
            load_queue=self.origin.list_couriers(self.BATCH_LIMIT)
            
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier in load_queue:
                self.stg.insert_courier(conn, courier)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.update_ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
