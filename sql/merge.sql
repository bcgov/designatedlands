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
  geom
)

SELECT
  $hierarchy AS hierarchy,
  '$desig_type'::TEXT AS designation,
  $source_id_col AS designation_id,
  $source_name_col AS designation_name,
  '$forest_restriction'::TEXT as forest_restriction,
  '$og_restriction'::TEXT as og_restriction,
  '$mine_restriction'::TEXT as mine_restriction,
  -- make sure the output is valid
  ST_Safe_Repair(
  -- dump
    (ST_Dump(
  -- merge records with the same name and id
       ST_Union(
  -- force to multipart just to make sure everthing is the same
         ST_Multi(geom)
      )
      )).geom) as geom
FROM $src_table
GROUP BY designation, designation_id, designation_name
;