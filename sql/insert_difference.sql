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

INSERT INTO $out_table ($columns, map_tile, geom)

WITH

src_clip AS
(SELECT
   $source_pk as id,
   $columns,
   map_tile,
   geom
 FROM $in_table
 WHERE map_tile LIKE %s
 $query),

dest_clip AS
(SELECT * FROM $out_table WHERE map_tile LIKE %s),

all_intersects AS
(SELECT
  i.id AS input_id,
  ST_Safe_Repair(ST_SnapToGrid(o.geom, .001)) as output_geom
FROM src_clip AS i
INNER JOIN dest_clip AS o
ON ST_Intersects(o.geom, i.geom)),

-- Union the existing intersecting polys in the output/target layer
-- To reduce topology exceptions, aggressively reduce precision of interesecting
-- records (1cm) because so many edges are similar/parallel
-- http://tsusiatsoftware.net/jts/jts-faq/jts-faq.html#D9
target_intersections AS
(SELECT
   a.input_id AS id,
   ST_Union(
      ST_Buffer(
         ST_CollectionExtract(
           ST_SnapToGrid(a.output_geom, 0.01), 3), 0)) AS geom
FROM all_intersects a
GROUP BY a.input_id),

difference AS (
SELECT
  id,
  $columns,
  map_tile,
  st_multi(st_union(geom)) AS geom
FROM
    (SELECT
       i.id as id,
       $columns,
       i.map_tile as map_tile,
       (ST_Dump(COALESCE(
          ST_Safe_Difference(i.geom, u.geom)
          ))).geom
        AS geom
     FROM src_clip AS i
     INNER JOIN target_intersections u
     ON i.id = u.id
     ) AS foo
-- discard very small differences
WHERE st_area(geom) > 10
GROUP BY foo.id, $columns, foo.map_tile
),

-- finally, non-intersecting records
non_intersections AS
(SELECT
  i.id,
  $columns,
  i.map_tile,
  ST_Multi(i.geom) as geom
FROM src_clip i
LEFT JOIN all_intersects a ON i.id = a.input_id
WHERE a.input_id IS null)


-- Combine inputs (difference and non-intersections)
-- Attempt to clean results of st_difference by:
--   - small in/out buffer
--   - snap to tile boundary to try and line up tile edges that have been shifted
--   - make sure we aren't inserting point or line intersections
SELECT
  $columns,
  d.map_tile,
  ST_Safe_Repair(
    ST_Snap(
      (ST_Dump(
        ST_Safe_Repair(
          ST_Buffer(
            ST_Buffer(d.geom, -0.001),
            .001)
          )
        )).geom,
      t.geom, .01
    )
  ) as geom
FROM difference d
INNER JOIN tiles t ON d.map_tile = t.map_tile
WHERE GeometryType(d.geom) = 'MULTIPOLYGON'
UNION ALL
SELECT
  $columns,
  ni.map_tile,
  ST_Safe_Repair(
    ST_Snap(
      (ST_Dump(
        ST_Safe_Repair(ni.geom)
        )).geom,
      t.geom, .01)
  ) as geom
FROM non_intersections ni
INNER JOIN tiles t ON ni.map_tile = t.map_tile
