CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers(
    id serial NOT NULL,
    id_courier varchar NOT NULL,
    object_value varchar NOT NULL,
    update_ts timestamp NOT null,
    CONSTRAINT couriers_pk PRIMARY KEY (id),
    CONSTRAINT couriers_id_unique UNIQUE (id_courier)
);