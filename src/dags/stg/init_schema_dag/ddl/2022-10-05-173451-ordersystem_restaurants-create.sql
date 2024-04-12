-- stg.ordersystem_restaurants definition
-- Drop table
-- DROP TABLE stg.ordersystem_restaurants;
CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT null,
    CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id),
	CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY (id)
);