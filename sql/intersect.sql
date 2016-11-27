INSERT INTO $out_table ($in_columns, $intersect_columns, map_tile, geom)

SELECT
  $in_columns,
  $intersect_columns,
  a.map_tile,
  CASE
    WHEN ST_CoveredBy(a.geom, ST_Buffer(b.geom, .01)) THEN a.geom
    ELSE ST_MakeValid(ST_CollectionExtract(ST_Intersection(a.geom, b.geom), 3))
  END as geom
FROM $in_table a
INNER JOIN $intersect_table b ON ST_Intersects(a.geom, b.geom)
WHERE a.map_tile LIKE %s AND b.map_tile LIKE %s;