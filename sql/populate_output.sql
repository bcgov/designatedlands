-- insert into output all areas in input that don't intersect with existing output

INSERT INTO $output
  (src_id, category, geom)

-- find all intersecting records
WITH all_intersects AS
(SELECT
  i.id,
  i.category,
  i.geom as input_geom,
  o.geom as output_geom
FROM
  $input AS i INNER JOIN
  $output AS o ON
  ST_Intersects(o.geom, i.geom))

-- find non-intersecting records
SELECT
  i.id,
  i.category,
  i.geom
FROM $input i
LEFT JOIN all_intersects a ON i.id = a.id
WHERE a.id IS null

UNION ALL

-- find rows that intersect but aren't completely covered
SELECT
  i.id,
  i.category,
  CASE
    WHEN ST_CoveredBy(i.geom, a.output_geom) THEN NULL
    ELSE ST_Multi(ST_Difference(i.geom, a.output_geom)
      ) END AS geom
 FROM
  $input AS i INNER JOIN
  all_intersects a ON i.id = a.id
