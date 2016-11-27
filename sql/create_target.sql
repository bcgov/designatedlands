-- create empty prelim output table

CREATE TABLE IF NOT EXISTS $table (
     $table_id serial PRIMARY KEY,
     designation text,
     map_tile text,
     geom geometry
);

-- index for speed
CREATE INDEX $table_gix ON $table USING GIST (geom);
CREATE INDEX $table_catix ON $table (designation);
CREATE INDEX $table_tileix ON $table (map_tile text_pattern_ops);