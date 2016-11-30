-- Clean input data
--   - union/merge all input geometries
--   - cut by tiles layer
--   - attempt to validate geometry

-- insert cleaned data
INSERT INTO $out_table ($columns, map_tile, geom)
SELECT
          $columns,
          b.map_tile,
  -- make sure the output is valid
          st_makevalid(
  -- dump and multipart
            ST_Multi((st_dump(
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
                  )).geom)) as geom
        FROM $src_table a
        INNER JOIN tiles b ON ST_Intersects(a.geom, b.geom);

-- index for speed
CREATE INDEX $out_table_gix ON $out_table USING GIST (geom);
CREATE INDEX $out_table_tileix ON $out_table (map_tile text_pattern_ops);
