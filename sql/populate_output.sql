-- insert into output all areas in input that don't intersect with existing output

INSERT INTO $output
  (src_id, category, geom)

-- Find all intersecting records immediately
-- This query is speedy, using the index - base all subqueries on the result
WITH all_intersects AS
(SELECT
  i.id AS input_id,
  o.id AS output_id,
  i.category AS input_category,
  -- snap new input poly within .1m of the exising output to the existing output
  --ST_MakeValid(ST_Snap(i.geom, o.geom, .1)) as input_geom,
  ST_MakeValid(o.geom) as output_geom
FROM
  $input AS i INNER JOIN
  $output AS o ON
  ST_Intersects(o.geom, i.geom)),

-- Union the existing intersecting polys in the output/target layer
-- To reduce topology exceptions, reduce precision of interesecting records
-- because many edges are similar/parallel
-- http://tsusiatsoftware.net/jts/jts-faq/jts-faq.html#D9
target_intersections AS
(SELECT
   a.input_id AS id,
   ST_MakeValid(
      ST_SnapToGrid(
         ST_Union(a.output_geom), .001)) AS geom
FROM all_intersects a
GROUP BY a.input_id),

difference AS (
SELECT
  id,
  category,
  st_multi(st_union(geom)) AS geom
FROM
    (SELECT
       i.id as id,
       i.category as category,
       (ST_Dump(COALESCE(
        ST_Difference(
             st_makevalid(
                st_buffer(
                   st_snap(
                      st_snaptogrid(i.geom, .01), u.geom, .1), 0)),

             st_makevalid(
                st_buffer(
                   st_snaptogrid(u.geom, .01), 0))
          )))).geom AS geom
     FROM $input AS i
     INNER JOIN target_intersections u
     ON i.id = u.id
     ) AS foo
WHERE st_area(geom) > 10
GROUP BY foo.id, foo.category
),


-- finally, non-intersecting records
non_intersections AS
(SELECT
  i.id,
  i.category,
  ST_Multi(i.geom) as geom
FROM $input i
LEFT JOIN all_intersects a ON i.id = a.input_id
WHERE a.input_id IS null)


-- combine things and make sure we aren't inserting point or line intersections
SELECT id, category, ST_MakeValid(geom) as geom
FROM difference
WHERE GeometryType(geom) = 'MULTIPOLYGON'
UNION ALL
SELECT id, category, ST_MakeValid(geom) as geom
FROM non_intersections