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

--   - remove gaps between source polys using buffer out / buffer in
--   - aggregate on designation (remove id and name)


-- create empty table with new auto-indexed id column
CREATE TABLE IF NOT EXISTS $out_table (
     id serial PRIMARY KEY,
     designation text,
     map_tile text,
     geom geometry
);

-- insert cleaned data
INSERT INTO $out_table (designation, map_tile, geom)
  SELECT foo.designation, foo.map_tile,
  -- snap result back to the tile boundaries
  st_safe_repair(st_snap(foo.geom, t.geom, .01)) as geom
  FROM (SELECT
          '$out_table'::TEXT AS designation,
          a.map_tile,
        -- Most inputs clean up easily, however some require a fairly
        -- thorough scrubbing to process without topology errors, hence the
        -- buffer out / buffer in

        -- buffer result back in to original position
          ST_Buffer(
          -- buffer out 3mm to remove any gaps between aggregated polys
            ST_Buffer(
          -- dump result of aggregation to singlepart
              (ST_Dump(
          -- aggregate
                 ST_Union(
          -- repair the result of snapping to grid / reducing precision
                   ST_Safe_Repair(
          -- reduce precision so that ST_Union does not choke on topo exceptions
                     ST_SnapToGrid(a.geom, .001)
                     )
                   )
                 )).geom,
              .003, 'join=mitre'),
            -.003) as geom
        FROM $src_table a
        GROUP BY a.designation, a.map_tile) AS foo
        INNER JOIN tiles t ON foo.map_tile = t.map_tile;

-- index for speed
CREATE INDEX $out_table_gix ON $out_table USING GIST (geom);
CREATE INDEX $out_table_tileix ON $out_table (map_tile text_pattern_ops);
