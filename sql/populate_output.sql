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
  i.geom as input_geom,
  o.geom as output_geom
FROM
  $input AS i INNER JOIN
  $output AS o ON
  ST_Intersects(o.geom, i.geom)),


-- find the difference of the intersectiong records
-- https://gis.stackexchange.com/questions/11592/difference-between-two-layers-in-postgis
intersections AS (
    SELECT
      id,
      category,
      st_multi(st_union(geom)) AS geom
    FROM
        (SELECT
           i.id,
           i.category,
           -- dump so we can filter on area, removing tiny slivers
           (st_dump(COALESCE(ST_Difference(i.geom, u.geom)))).geom  AS geom
         FROM $input AS i
         -- join to back to the intersection, unioning the intersecting polys
         -- in the output layer. Because of parallel/similar edges and the fact
         -- that the join means the overlap returned may be many features, the
         -- intersection requires a bit of massaging to make sure things are
         -- valid (slivers)
         INNER JOIN
           (SELECT
              a.input_id AS id,
              ST_Buffer(
                ST_Snaptogrid(
                  ST_Union(a.output_geom), 0.001), 0) AS geom
            FROM all_intersects a
            GROUP BY a.input_id) u
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
SELECT id, category, geom
FROM intersections
WHERE GeometryType(geom) = 'MULTIPOLYGON'
UNION ALL
SELECT *
FROM non_intersections