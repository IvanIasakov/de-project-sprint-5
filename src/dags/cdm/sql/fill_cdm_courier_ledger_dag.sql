DELETE FROM cdm.dm_courier_ledger where settlement_year=date_part('year', '{{ds}}'::timestamp) and settlement_month=date_part('month', '{{ds}}'::timestamp);
INSERT INTO cdm.dm_courier_ledger
(courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
(with CRDEL as 
(select cr.courier_id ,cr.courier_name , ts."year" as settlement_year ,ts."month" as settlement_month , dv.order_id ,ord.order_status
,dv.rate, AVG(dv.rate) over (PARTITION BY dv.courier_id ,ts."year" ,ts."month" ) ar
,dv.tip_sum ,dv.total_sum 
from  dds.dm_deliverys as dv
left join dds.dm_orders ord on dv.order_id = ord.id 
left join dds.dm_timestamps ts on ord.timestamp_id=ts.id
left join dds.dm_couriers cr on dv.courier_id = cr.id 
where ts.ts>=date_trunc('month','{{ds}}'::timestamp) and ts.ts<date_trunc('month','{{ds}}'::timestamp + interval '1 month')
--and ord.order_status!='CANCELLED'
),
CRDELSUM as
(select 
CD.courier_id,cd.courier_name,cd.settlement_year,cd.settlement_month,
count(distinct cd.order_id) as orders_count,
sum(cd.total_sum) as orders_total_sum,
avg(cd.rate) as rate_avg,
sum(cd.total_sum)*0.25 as order_processing_fee,
sum(
case when CD.ar<4 and CD.total_sum*0.05<=100 then 100
     when CD.ar<4 and CD.total_sum*0.05>100 then CD.total_sum*0.05
     when CD.ar>=4 and CD.ar<4.5 and CD.total_sum*0.07<=150 then 150
     when CD.ar>=4 and CD.ar<4.5 and CD.total_sum*0.07>150 then CD.total_sum*0.07
     when CD.ar>=4.5 and CD.ar<4.9 and CD.total_sum*0.08<=175 then 175
     when CD.ar>=4.5 and CD.ar<4.9 and CD.total_sum*0.08>175 then CD.total_sum*0.08
     when CD.ar>=4.9 and CD.total_sum*0.1<=200 then 200
     when CD.ar>=4.9 and CD.total_sum*0.1>200 then CD.total_sum*0.1 end) as courier_order_sum,
sum(cd.tip_sum) as courier_tips_sum
from CRDEL as CD group by CD.courier_id,cd.courier_name,cd.settlement_year,cd.settlement_month)
select 
DCL.courier_id,
DCL.courier_name,
DCL.settlement_year,
DCL.settlement_month,
DCL.orders_count,
DCL.orders_total_sum,
DCL.rate_avg,
DCL.order_processing_fee,
DCL.courier_order_sum,
DCL.courier_tips_sum,
DCL.courier_order_sum+DCL.courier_tips_sum*0.95 as courier_reward_sum
from CRDELSUM as DCL
order by DCL.settlement_year,DCL.settlement_month,DCL.courier_name)
ON CONFLICT (courier_id, settlement_year, settlement_month)
DO UPDATE 
SET 
courier_name=EXCLUDED.courier_name,
orders_count=EXCLUDED.orders_count, 
orders_total_sum=EXCLUDED.orders_total_sum, 
rate_avg=EXCLUDED.rate_avg, 
order_processing_fee=EXCLUDED.order_processing_fee, 
courier_order_sum=EXCLUDED.courier_order_sum, 
courier_tips_sum=EXCLUDED.courier_tips_sum, 
courier_reward_sum=EXCLUDED.courier_reward_sum;

