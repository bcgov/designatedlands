-- create empty output table

CREATE UNLOGGED TABLE IF NOT EXISTS $table (
     conservation_lands_id serial PRIMARY KEY,
     rollup text,
     category text,
     terrestrial text,
     map_tile text,
     geom geometry
);

-- index for speed
CREATE INDEX $table_geom_ix ON $table USING GIST (geom);
CREATE INDEX $table_category_ix ON $table (category);
CREATE INDEX $table_maptile_ix ON $table (map_tile text_pattern_ops);