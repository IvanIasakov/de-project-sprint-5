-- cdm.dm_courier_ledger definition
-- Drop table
-- DROP TABLE cdm.dm_courier_ledger;
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger(
	id serial4,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int4 not null,
    settlement_month int4 not null,
	orders_count integer DEFAULT 0 NOT NULL,
	orders_total_sum numeric(14, 2) DEFAULT 0::numeric NOT NULL,
	rate_avg numeric not null,
	order_processing_fee numeric(14, 2) DEFAULT 0::numeric NOT NULL,
	courier_order_sum numeric(14, 2) DEFAULT 0::numeric NOT NULL,
	courier_tips_sum numeric(14, 2) DEFAULT 0::numeric NOT NULL,
	courier_reward_sum numeric(14, 2)DEFAULT 0::numeric NOT null,
    CONSTRAINT dm_courier_ledger_pk primary key (id),
	CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month),
	CONSTRAINT dm_courier_ledger_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
	CONSTRAINT dm_courier_ledger_year_check CHECK (((settlement_year >= 2022) AND (settlement_year < 2500))),
	CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK ((order_processing_fee >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK ((courier_reward_sum >= (0)::numeric)),
	CONSTRAINT dm_courier_ledger_orders_count_check CHECK ((orders_count >= (0)))
);
CREATE UNIQUE INDEX IF NOT EXISTS id_cdm_dm_cl ON cdm.dm_courier_ledger USING btree (id);