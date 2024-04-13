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
from datetime import datetime,timedelta
import requests
import time

class DeliveryObj(BaseModel):
    id: int
    delivery_id: str
    object_value: str 
    update_ts: datetime


class DeliveryOriginRepository:
    def __init__(self,log: Logger) -> None:
         self.log=log

    def list_deliverys(self) -> List[DeliveryObj]:
                daysnum = int(Variable.get('DELIVERY_SYSTEM_DAYS_LOAD'))
                limit = int(Variable.get('DELIVERY_SYSTEM_BATCH_LIMIT'))
                from_dt=(datetime.now()-timedelta(days=daysnum)).replace(microsecond=0) # начальная дата выгрузки
                to_dt=datetime.now().replace(microsecond=0) # конечная дата выгрузки 
                api_conn = BaseHook.get_connection('DELIVERY_SYSTEM_API')
                x_api_key = Variable.get('DELIVERY_SYSTEM_X-API-KEY')
                x_nickname= Variable.get('DELIVERY_SYSTEM_X-Nickname')
                x_cohotrt = Variable.get('DELIVERY_SYSTEM_X-Cohort')
                X_HEADERS={"X-API-KEY": x_api_key,
                           "X-Nickname": x_nickname,
                           "X-Cohort": x_cohotrt}
                api_endpoint = api_conn.host
                method_url = '/deliveries'
                api_url='https://'+ api_endpoint + method_url
                OObj=[]
                offset=0
                while True:
                    x_params = {
                           'from':from_dt
                          ,'to':to_dt
                          ,'sort_field': "date"
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
                         OObj.append(DeliveryObj(**{'id':0,'delivery_id':el['delivery_id'],'object_value':str(el).replace("'","\""),'update_ts':datetime.now()}))
                    
                    if len(list_response)<limit:
                         break                
                return OObj



class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliverys(delivery_id,object_value,update_ts)
                    VALUES (%(delivery_id)s, %(object_value)s, %(update_ts)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,update_ts=EXCLUDED.update_ts;
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "object_value": delivery.object_value,
                    "update_ts": delivery.update_ts
                },
            )


class DeliveryLoader:
    WF_KEY = "delivery_deliverys_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ts"
    BATCH_LIMIT = 50  # Читаем по кусочками из API - размер кусочка.

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveryOriginRepository(log)
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliverys(self,daysnum: int):
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
            
            load_queue=self.origin.list_deliverys()
                        
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
