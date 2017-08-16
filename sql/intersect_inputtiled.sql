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

-- intersect where
--  - input table is tiled by map_tile
--  - intersect table is not tiled
-- tile table is required (cutting intersect by tile isn't necessary but
--  probably doesn't hurt in case of very large geometries)

INSERT INTO $out_table ($in_columns, $intersect_columns, map_tile, geom)

WITH

intersect_tile AS
(SELECT $intersect_columns,
  CASE
    WHEN ST_CoveredBy(ST_CollectionExtract(i.geom, 3), ST_CollectionExtract(tile.geom, 3)) THEN ST_MakeValid(i.geom)
    ELSE ST_CollectionExtract(
             ST_Intersection(
                  ST_MakeValid(
                      ST_SnapToGrid(
                           ST_Buffer(i.geom, 0), .001)), tile.geom),  3)
  END as geom
  FROM $intersect_table i
  INNER JOIN $tile_table tile ON ST_Intersects(ST_CollectionExtract(i.geom, 3), ST_CollectionExtract(tile.geom, 3))
 WHERE tile.map_tile LIKE %s)



SELECT
  $in_columns,
  $intersect_columns,
  b.map_tile,
  CASE
    WHEN ST_CoveredBy(a.geom, ST_Buffer(b.geom, .01)) THEN ST_MakeValid(a.geom)
    ELSE ST_MakeValid(
            ST_CollectionExtract(
               ST_Intersection(
                  ST_MakeValid(a.geom), ST_MakeValid(
                                            ST_SnapToGrid(b.geom, .001))
                               )
               , 3)
            )
  END as geom
FROM intersect_tile a
INNER JOIN $in_table b ON ST_Intersects(ST_CollectionExtract(a.geom, 3), ST_CollectionExtract(b.geom, 3))
WHERE b.map_tile LIKE %s;
