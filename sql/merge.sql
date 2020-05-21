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

--   - merge/repair data in src_table
--   - where available, retain source id and source name

-- insert cleaned data plus restriction columns


INSERT INTO $out_table (
  hierarchy,
  designation,
  source_id,
  source_name,
  forest_restriction,
  og_restriction,
  mine_restriction,
  map_tile,
  geom
)

SELECT
  $hierarchy AS hierarchy,
  '$desig_type'::TEXT AS designation,
  a.$source_id_col AS designation_id,
  a.$source_name_col AS designation_name,
  $forest_restriction as forest_restriction,
  $og_restriction as og_restriction,
  $mine_restriction as mine_restriction,
  b.map_tile,
  -- make sure the output is valid
  ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- merge records with the same name and id
       ST_Union(
  -- force to multipart just to make sure everthing is the same
         ST_Multi(
  -- polygons only
            ST_CollectionExtract(
  -- intersect with tiles on land
                  CASE
                    WHEN ST_CoveredBy(a.geom, b.geom) THEN a.geom
                    ELSE ST_Safe_Intersection(a.geom, b.geom)
                  END
                , 3)

          )
      )
      )).geom) as geom
FROM $src_table a
INNER JOIN designatedlands.bc_boundary b
ON ST_Intersects(a.geom, b.geom)
WHERE b.bc_boundary = 'bc_boundary_land'
GROUP BY designation, designation_id, designation_name, map_tile
;