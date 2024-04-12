-- dds.dm_deliverys definition

-- Drop table

-- DROP TABLE dds.dm_deliverys;

CREATE TABLE IF NOT EXISTS dds.dm_deliverys (
	id serial4 NOT NULL,
	courier_id integer NOT NULL,
	order_id integer NOT NULL,
	delivery_key varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	address varchar NOT NULL,
	rate  integer NOT NULL,
	tip_sum numeric (14, 2) NOT NULL,
	total_sum numeric (14, 2) NOT NULL,
	CONSTRAINT dm_deliverys_pkey PRIMARY KEY (id),
	CONSTRAINT dm_deliverys_delivery_key_unique UNIQUE (delivery_key),
	CONSTRAINT dm_deliverys_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
    CONSTRAINT dm_deliverys_courier_id_fk FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
    CONSTRAINT dm_deliveries_rate_check CHECK (((rate >= 1) AND (rate <= 5)))

);