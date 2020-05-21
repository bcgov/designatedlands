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

-- create empty prelim output table

DROP TABLE IF EXISTS designatedlands.bc_boundary;

CREATE TABLE designatedlands.bc_boundary (
     bc_boundary_id serial PRIMARY KEY,
     designation text,
     map_tile text,
     geom geometry
);

-- index for speed
CREATE INDEX ON designatedlands.bc_boundary USING GIST (geom);
CREATE INDEX ON designatedlands.bc_boundary (map_tile text_pattern_ops);
