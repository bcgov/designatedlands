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

-- Create empty prelim output tables

-- overlaps table (simply dump all sources into this one)
DROP TABLE IF EXISTS $table_overlaps;
CREATE TABLE $table_overlaps (
     $table_overlaps_prelim_id serial PRIMARY KEY,
     designation text,
     designation_id text,
     designation_name text,
     map_tile text,
     geom geometry
);

-- index for speed
CREATE INDEX IF NOT EXISTS $table_overlaps_gix ON $table_overlaps USING GIST (geom);
CREATE INDEX IF NOT EXISTS $table_overlaps_catix ON $table_overlaps (designation);
CREATE INDEX IF NOT EXISTS $table_overlaps_desidtix ON $table_overlaps (designation_id);
CREATE INDEX IF NOT EXISTS $table_overlaps_desnmtix ON $table_overlaps (designation_name);
CREATE INDEX IF NOT EXISTS $table_overlaps_tileix ON $table_overlaps (map_tile text_pattern_ops);

-- create no overlaps table (overlapping sources are removed based on hierarchy)
DROP TABLE IF EXISTS $table_prelim;
CREATE TABLE $table_prelim (
     $table_prelim_id serial PRIMARY KEY,
     designation text,
     map_tile text,
     geom geometry
);

-- index for speed
CREATE INDEX IF NOT EXISTS $table_prelim_gix ON $table_prelim USING GIST (geom);
CREATE INDEX IF NOT EXISTS $table_prelim_catix ON $table_prelim (designation);
CREATE INDEX IF NOT EXISTS $table_prelim_tileix ON $table_prelim (map_tile text_pattern_ops);