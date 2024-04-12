CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliverys(
    id serial NOT NULL,
    delivery_id varchar NOT NULL,
    object_value varchar NOT NULL,
    update_ts timestamp NOT null,
    CONSTRAINT deliverys_pk PRIMARY KEY (id),
    CONSTRAINT deliverys_id_unique UNIQUE (delivery_id)
);