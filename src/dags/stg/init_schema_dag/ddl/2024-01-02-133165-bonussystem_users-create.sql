-- stg.bonussystem_users definition
-- Drop table
-- DROP TABLE stg.bonussystem_users;
CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);