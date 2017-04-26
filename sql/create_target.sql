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

-- create empty prelim output table

CREATE TABLE IF NOT EXISTS $table (
     $table_id serial PRIMARY KEY,
     designation text,
     map_tile text,
     geom geometry
);

-- index for speed
CREATE INDEX $table_gix ON $table USING GIST (geom);
CREATE INDEX $table_catix ON $table (designation);
CREATE INDEX $table_tileix ON $table (map_tile text_pattern_ops);
