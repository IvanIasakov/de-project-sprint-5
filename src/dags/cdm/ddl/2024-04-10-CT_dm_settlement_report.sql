-- cdm.dm_settlement_report definition

-- Drop table

-- DROP TABLE cdm.dm_settlement_report;

CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	settlement_date date NOT NULL,
	orders_count int4 DEFAULT 0 NOT NULL,
	orders_total_sum numeric(14, 2) DEFAULT 0::numeric NOT NULL,
	orders_bonus_payment_sum numeric(14, 2) DEFAULT 0::numeric NOT NULL,
	orders_bonus_granted_sum numeric(14, 2) DEFAULT 0::numeric NOT NULL,
	order_processing_fee numeric(14, 2) DEFAULT 0::numeric NOT NULL,
	restaurant_reward_sum numeric(14, 2) DEFAULT 0::numeric NOT NULL,
	CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK ((orders_bonus_granted_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK ((orders_bonus_payment_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_pkey PRIMARY KEY (id),
	CONSTRAINT dm_settlement_report_restaurant_orders_count_check CHECK ((orders_count >= 0)),
	CONSTRAINT dm_settlement_report_restaurant_reward_sum_check CHECK ((restaurant_reward_sum >= (0)::numeric)),
	CONSTRAINT dm_settlement_report_restaurant_unique_check UNIQUE (restaurant_id, settlement_date),
	CONSTRAINT dm_settlement_report_settlement_date_check CHECK (((settlement_date >= '2022-01-01'::date) AND (settlement_date < '2500-01-01'::date)))
);
CREATE UNIQUE INDEX IF NOT EXISTS id1 ON cdm.dm_settlement_report USING btree (id);