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

INSERT INTO $out_table ($in_columns, $intersect_columns, map_tile, geom)


SELECT
  $in_columns,
  $intersect_columns,
  a.map_tile,
  CASE
    WHEN ST_CoveredBy(a.geom, ST_Buffer(b.geom, .01)) THEN ST_MakeValid(a.geom)
    ELSE ST_MakeValid(ST_Multi(ST_CollectionExtract(ST_Intersection(
                     ST_MakeValid(a.geom), ST_MakeValid(b.geom)
                     ), 3)))
   END as geom
FROM $in_table a
INNER JOIN $intersect_table b ON ST_Intersects(a.geom, b.geom)
WHERE a.map_tile LIKE %s AND b.map_tile LIKE %s
