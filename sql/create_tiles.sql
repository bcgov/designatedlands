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

-- Some marine portions of input data falls outside of the 20k tiles (such as
-- Conservancies) To fill the gaps, create a tile layer that includes 250k
-- tiles for marine areas (except for SSOG to avoid confusion with USA border)

DROP TABLE IF EXISTS tiles;
CREATE TABLE tiles (tile_id serial primary key, map_tile text, geom geometry);

CREATE INDEX ON tiles (map_tile);
CREATE INDEX tiles_gidx ON tiles USING GIST (geom) ;

INSERT INTO tiles (map_tile, geom)
SELECT map_tile, geom FROM a00_tiles_20k;

INSERT INTO tiles (map_tile, geom)
SELECT a.map_tile||'000',
ST_Multi(ST_CollectionExtract(ST_Safe_Difference(a.geom, ST_Union(b.geom)), 3))
FROM a00_tiles_250k a
INNER JOIN a00_tiles_20k b
ON ST_Intersects(a.geom, b.geom)
WHERE a.map_tile IN
  ('092E','092C','102I','102O','102P','103A','103B',
   '103F','103G', '103H', '103C','103J','103K')
GROUP BY a.map_tile, a.geom;

