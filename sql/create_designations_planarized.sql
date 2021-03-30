-- convert overlapping designatedlands polys to lines
WITH lines AS
(
  SELECT
    map_tile,
    ST_Union(ST_ExteriorRing(geom)) AS geom
  FROM designations_overlapping
  WHERE map_tile LIKE %s
  GROUP BY map_tile
),

-- polygonize the resulting noded lines
flattened AS
(
  SELECT
    map_tile,
    (ST_Dump(ST_Polygonize(geom))).geom AS geom
  FROM lines
  GROUP BY map_tile
),

-- get the attributes and sort by hierarchy
sorted AS
(
  SELECT
    d.hierarchy,
    d.designation,
    d.source_id,
    d.source_name,
    d.forest_restriction,
    d.mine_restriction,
    d.og_restriction,
    f.map_tile,
    f.geom
  FROM flattened f
  INNER JOIN designations_overlapping d
  ON ST_Contains(d.geom, ST_PointOnSurface(f.geom))
  ORDER BY d.hierarchy, d.source_id
)

INSERT INTO designations_planarized (
  hierarchy,
  designation,
  source_id,
  source_name,
  forest_restriction,
  mine_restriction,
  og_restriction,
  map_tile,
  geom
)
SELECT
  array_agg(hierarchy ORDER BY hierarchy) as hierarchy,
  array_agg(designation ORDER BY hierarchy) as designation,
  array_agg(source_id ORDER BY hierarchy) as source_id,
  array_agg(source_name ORDER BY hierarchy) as source_name,
  array_agg(forest_restriction ORDER BY hierarchy) as forest_restriction,
  array_agg(mine_restriction ORDER BY hierarchy) as mine_restriction,
  array_agg(og_restriction ORDER BY hierarchy) as og_restriction,
  map_tile,
  geom
FROM sorted
GROUP BY map_tile, geom;
