-- create empty output table and index the geometry

CREATE UNLOGGED TABLE IF NOT EXISTS $output (
     id serial PRIMARY KEY,
     src_id integer,
     category text,
     geom geometry
);

CREATE INDEX $output_gix ON $output USING GIST (geom);