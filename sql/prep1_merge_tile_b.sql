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

-- ----------------------------------------------------------------------------------------------------

--   - tile/merge/repair data in src_table
--   - do not retain source id / source name


-- create empty table with new auto-indexed id column
CREATE TABLE IF NOT EXISTS $out_table (
     id serial PRIMARY KEY,
     designation text,
     map_tile text,
     geom geometry
);

-- insert cleaned data
INSERT INTO $out_table (designation, map_tile, geom)
  SELECT designation, map_tile, geom
  FROM (SELECT
          '$out_table'::TEXT AS designation,
          b.map_tile,
          -- make sure the output is valid
          ST_Safe_Repair(
          -- dump
            (ST_Dump(
          -- union to remove overlapping polys within the source
            ST_Union(
              ST_Multi(
          -- include only polygons in cases of geometrycollections
                ST_CollectionExtract(
          -- intersect with tiles
                  CASE
                    WHEN ST_CoveredBy(a.geom, b.geom) THEN a.geom
                    ELSE ST_Safe_Intersection(a.geom, b.geom)
                  END
                , 3)
                )
              )
              )).geom) as geom
        FROM $src_table a
        INNER JOIN tiles b ON ST_Intersects(a.geom, b.geom)
        GROUP BY designation, map_tile) AS foo;

-- index for speed
CREATE INDEX $out_table_gix ON $out_table USING GIST (geom);
CREATE INDEX $out_table_tileix ON $out_table (map_tile text_pattern_ops);
