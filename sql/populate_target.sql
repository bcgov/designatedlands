INSERT INTO $out_table (designation, map_tile, geom)

WITH

src_clip AS
(SELECT
   id,
   designation,
   map_tile,
   geom
 FROM $in_table
 WHERE map_tile LIKE %s),

dest_clip AS
(SELECT * FROM $out_table WHERE map_tile LIKE %s),

all_intersects AS
(SELECT
  i.id AS input_id,
  i.designation AS input_designation,
  ST_MakeValid(
     ST_SnapToGrid(o.geom, 0.001)) as output_geom
FROM src_clip AS i
INNER JOIN dest_clip AS o
ON ST_Intersects(o.geom, i.geom)),

-- Union the existing intersecting polys in the output/target layer
-- To reduce topology exceptions, reduce precision of interesecting records
-- because many edges are similar/parallel
-- http://tsusiatsoftware.net/jts/jts-faq/jts-faq.html#D9
target_intersections AS
(SELECT
   a.input_id AS id,
   ST_Union(
       ST_Buffer(
         ST_CollectionExtract(
           ST_SnapToGrid(a.output_geom, .001), 3), 0.001)) AS geom
FROM all_intersects a
GROUP BY a.input_id),

difference AS (
SELECT
  id,
  designation,
  map_tile,
  st_multi(st_union(geom)) AS geom
FROM
    (SELECT
       i.id as id,
       i.designation as designation,
       i.map_tile as map_tile,
       (ST_Dump(COALESCE(
        -- no exception catching
        /*
        ST_Difference(
             st_makevalid(
                st_buffer(
                   st_snap(
                      st_snaptogrid(i.geom, .01), u.geom, 1), 0)),

             st_makevalid(
                st_buffer(
                   st_snaptogrid(u.geom, .01), 0)))
        */

        -- catch exceptions
          safe_diff(i.geom, u.geom)
          ))).geom
        AS geom
     FROM src_clip AS i
     INNER JOIN target_intersections u
     ON i.id = u.id
     ) AS foo
-- discard very small differences
WHERE st_area(geom) > 10
GROUP BY foo.id, foo.designation, foo.map_tile
),


-- finally, non-intersecting records
non_intersections AS
(SELECT
  i.id,
  i.designation,
  i.map_tile,
  ST_Multi(i.geom) as geom
FROM src_clip i
LEFT JOIN all_intersects a ON i.id = a.input_id
WHERE a.input_id IS null)


-- combine things and make sure we aren't inserting point or line intersections
SELECT designation, map_tile, ST_MakeValid(geom) as geom
FROM difference
WHERE GeometryType(geom) = 'MULTIPOLYGON'
UNION ALL
SELECT designation, map_tile, ST_MakeValid(geom) as geom
FROM non_intersections