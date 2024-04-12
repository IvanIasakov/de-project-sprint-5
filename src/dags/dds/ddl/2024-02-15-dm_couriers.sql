-- dds.dm_couriers definition

-- Drop table

-- DROP TABLE dds.dm_couriers;

CREATE TABLE IF NOT EXISTS dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id),
	CONSTRAINT dm_couriers_courier_id_unique UNIQUE (courier_id)

);