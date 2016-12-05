-- intersect where
--  - input table is tiled by map_tile
--  - intersect table is not tiled
-- tile table is required (cutting intersect by tile isn't necessary but
--  probably doesn't hurt in case of very large geometries)

INSERT INTO $out_table ($in_columns, $intersect_columns, map_tile, geom)

WITH

intersect_tile AS
(SELECT $intersect_columns,
  CASE
    WHEN ST_CoveredBy(ST_CollectionExtract(i.geom, 3), tile.geom) THEN ST_MakeValid(i.geom)
    ELSE ST_CollectionExtract(
             ST_Intersection(
                  ST_MakeValid(
                      ST_SnapToGrid(
                           ST_Buffer(i.geom, 0), .001)), tile.geom),  3)
  END as geom
  FROM $intersect_table i
  INNER JOIN $tile_table tile ON ST_Intersects(i.geom, tile.geom)
 WHERE tile.map_tile LIKE %s)



SELECT
  $in_columns,
  $intersect_columns,
  b.map_tile,
  CASE
    WHEN ST_CoveredBy(a.geom, ST_Buffer(b.geom, .01)) THEN ST_MakeValid(a.geom)
    ELSE ST_MakeValid(ST_CollectionExtract(ST_Intersection(a.geom, b.geom), 3))
  END as geom
FROM intersect_tile a
INNER JOIN $in_table b ON ST_Intersects(a.geom, b.geom)
WHERE b.map_tile LIKE %s;