-- create empty output table

CREATE UNLOGGED TABLE IF NOT EXISTS $output (
     id serial PRIMARY KEY,
     src_id integer,
     category text,
     geom geometry
)