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

-- overlay (intersect) two tables for given tile

INSERT INTO $out_table ($columns_a, $columns_b, geom)

WITH

tile AS
(
  SELECT geom
  FROM $tile_table WHERE map_tile LIKE %s
),

tile_a AS
(
  SELECT $columns_a,
    CASE
      WHEN ST_CoveredBy(ST_CollectionExtract(a.geom, 3), ST_CollectionExtract(tile.geom, 3)) THEN ST_MakeValid(a.geom)
      ELSE ST_CollectionExtract(
               ST_Intersection(
                    ST_MakeValid(
                        ST_SnapToGrid(
                             ST_Buffer(a.geom, 0), 0.001)), tile.geom),  3)
    END as geom
  FROM $table_a a
  INNER JOIN tile ON ST_Intersects(ST_CollectionExtract(a.geom, 3), ST_CollectionExtract(tile.geom, 3))
),

tile_b AS
(
  SELECT $columns_b,
    CASE
      WHEN ST_CoveredBy(ST_CollectionExtract(b.geom, 3), ST_CollectionExtract(tile.geom, 3)) THEN ST_MakeValid(b.geom)
      ELSE ST_CollectionExtract(
               ST_Intersection(
                    ST_MakeValid(
                        ST_SnapToGrid(
                             ST_Buffer(b.geom, 0), 0.001)), tile.geom),  3)
    END as geom
  FROM $table_b b
  INNER JOIN tile ON ST_Intersects(ST_CollectionExtract(b.geom, 3), ST_CollectionExtract(tile.geom, 3))
 )

SELECT
  $columns_a,
  $columns_b,
  CASE
    WHEN ST_CoveredBy(a.geom, ST_Buffer(b.geom, .01)) THEN ST_MakeValid(a.geom)
    ELSE ST_MakeValid(
            ST_CollectionExtract(
               ST_Intersection(
                  ST_MakeValid(a.geom), ST_MakeValid(
                                            ST_SnapToGrid(b.geom, 0.001))
                               )
               , 3)
            )
  END as geom
FROM tile_a a
INNER JOIN tile_b b ON ST_Intersects(ST_CollectionExtract(a.geom, 3), ST_CollectionExtract(b.geom, 3));
