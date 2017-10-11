-- Copyright 2017 Province of British Columbia
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--
-- See the License for the specific language governing permissions and limitations under the License.

-- Clean input data
--   - validate geometry
--   - cut by tiles layer


-- create empty table with new auto-indexed id column
CREATE UNLOGGED TABLE IF NOT EXISTS $out_table (
     id serial PRIMARY KEY,
     designation text,
     designation_id text,
     designation_name text,
     map_tile text,
     geom geometry
);

-- insert cleaned data
INSERT INTO $out_table (designation, designation_id, designation_name, map_tile, geom)
  SELECT designation, designation_id, designation_name, map_tile, geom
  FROM (SELECT
          '$out_table'::TEXT as designation,
          a.$designation_id_col as designation_id,
          a.$designation_name_col as designation_name,
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
                        ELSE ST_Intersection(ST_MakeValid(a.geom), b.geom)
                      END
                    , 3)
                  )
                , 0)
                )
              )
            )).geom) as geom
        FROM $src_table a
        INNER JOIN tiles b ON ST_Intersects(a.geom, b.geom)
        GROUP BY designation, designation_id, designation_name, map_tile) as foo;

-- index for speed
CREATE INDEX $out_table_gix ON $out_table USING GIST (geom);
CREATE INDEX $out_table_tileix ON $out_table (map_tile text_pattern_ops);
