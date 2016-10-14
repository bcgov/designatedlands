-- create empty output table and index the geometry

CREATE UNLOGGED TABLE IF NOT EXISTS $table (
     id serial PRIMARY KEY,
     src_id integer,
     category text,
     geom geometry
);
