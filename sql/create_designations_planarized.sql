-- load source polygons plus tile boundary
WITH src AS
(
  SELECT
    map_tile,
    geom
  FROM designations_overlapping
  WHERE map_tile LIKE %s
  UNION ALL
  SELECT
    map_tile,
    geom
  FROM bc_boundary_land_tiled
  WHERE map_tile LIKE %s
),

-- dump poly rings and convert to lines
rings as
(
  SELECT
    map_tile,
    ST_Exteriorring((ST_DumpRings(geom)).geom) AS geom
  FROM src
),

-- node the lines with st_union and dump to singlepart lines
lines as
(
  SELECT
    map_tile,
    (st_dump(st_union(geom, .1))).geom as geom
  FROM rings
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

-- get the attributes and sort by process_order
sorted AS
(
  SELECT
    d.process_order,
    d.designation,
    d.source_id,
    d.source_name,
    COALESCE(d.forest_restriction, 0) as forest_restriction,
    COALESCE(d.mine_restriction, 0) as mine_restriction,
    COALESCE(d.og_restriction, 0) as og_restriction,
    f.map_tile,
    f.geom
  FROM flattened f
  LEFT OUTER JOIN designations_overlapping d
  ON ST_Contains(d.geom, ST_PointOnSurface(f.geom))
  ORDER BY d.process_order, d.source_id
)

INSERT INTO designations_planarized (
  process_order,
  designation,
  source_id,
  source_name,
  forest_restrictions,
  mine_restrictions,
  og_restrictions,
  forest_restriction_max,
  mine_restriction_max,
  og_restriction_max,
  map_tile,
  geom
)
SELECT
  array_agg(process_order ORDER BY process_order) as process_order,
  array_agg(designation ORDER BY process_order) as designation,
  array_agg(source_id ORDER BY process_order) as source_id,
  array_agg(source_name ORDER BY process_order) as source_name,
  array_agg(forest_restriction ORDER BY process_order) as forest_restrictions,
  array_agg(mine_restriction ORDER BY process_order) as mine_restrictions,
  array_agg(og_restriction ORDER BY process_order) as og_restrictions,
  (sort_desc(array_agg(forest_restriction)))[1] as forest_restriction_max,
  (sort_desc(array_agg(mine_restriction)))[1] as mine_restriction_max,
  (sort_desc(array_agg(og_restriction)))[1] as og_restriction_max,
  map_tile,
  geom
FROM sorted
GROUP BY map_tile, geom;
