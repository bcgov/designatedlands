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

-- insert cleaned data
INSERT INTO $out_table (
  designation,
  designation_id,
  designation_name,
  geom
)
SELECT
  '$out_table'::TEXT AS designation,
  a.$designation_id_col AS designation_id,
  a.$designation_name_col AS designation_name,
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
FROM $src_table a
GROUP BY designation, designation_id, designation_name
;