-- Clean input data
-- (simply union all features and try and try to make sure they are vaild)

-- create empty table with new auto-indexed id column
CREATE UNLOGGED TABLE IF NOT EXISTS $out_table (
     id serial PRIMARY KEY,
     category text,
     map_tile text,
     geom geometry
);

-- insert cleaned data
INSERT INTO $out_table (category, map_tile, geom)
  SELECT category, map_tile, geom
  FROM (SELECT
          '$out_table'::TEXT as category,
          b.map_tile,
  -- make sure the output is valid
          st_makevalid(
  -- dump
            (st_dump(
  -- union to remove overlapping polys within the source
            ST_Union(
  -- make buffer result multipart
                ST_Multi(
  -- buffer the features by 0 to help with validity
                  ST_Buffer(
  -- first validity check
                    ST_MakeValid(
  -- include only polygons in cases of geometrycollections
                      ST_CollectionExtract(
  -- intersect with tiles
                        CASE
                          WHEN ST_CoveredBy(a.geom, b.geom) THEN a.geom
                          ELSE ST_Intersection(a.geom, b.geom)
                        END
                      , 3)
                    )
                  , 0)
                  )
                )
            )).geom) as geom
        FROM $src_table a
        INNER JOIN tiles b ON ST_Intersects(a.geom, b.geom)
        GROUP BY category, map_tile) as foo;

-- index for speed
CREATE INDEX $out_table_gix ON $out_table USING GIST (geom);
CREATE INDEX $out_table_tileix ON $out_table (map_tile text_pattern_ops);
