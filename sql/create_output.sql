-- create empty output table

CREATE UNLOGGED TABLE IF NOT EXISTS $table (
     id serial PRIMARY KEY,
     src_id integer,
     category text,
     map_tile text,
     geom geometry
);

-- index for speed
CREATE INDEX $table_gix ON $table USING GIST (geom);
CREATE INDEX $table_catix ON $table (category);
CREATE INDEX $table_tileix ON $table (map_tile text_pattern_ops);