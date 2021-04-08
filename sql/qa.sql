-- run a few basic area comparisons to check that outputs make sense


-- first, compare total designation area in the _overlaps and _planarized tables
-- these are expected to be similar - but not equal. This is because overlaps within
-- designations occur (and can be extensive). This query shows just how extensive
-- these overlaps can be.
DROP TABLE IF EXISTS qa_compare_outputs;

CREATE TABLE qa_compare_outputs AS

WITH overlap_summary AS
(
SELECT
 array[designation] as designation,
 SUM(st_area(geom)) / 10000 as area_ha
FROM designations_overlapping
GROUP BY designation
ORDER BY designation
),

distinct_combinations AS
(SELECT
 designation,
 SUM(st_area(geom)) / 10000 as area_ha_planarized
FROM designations_planarized
GROUP BY designation
ORDER BY designation)

SELECT
  o.designation,
  ROUND(o.area_ha::numeric, 2) as area_ha_overlap,
  ROUND(SUM(p.area_ha_planarized)::numeric, 2) as area_ha_planarized,
  ROUND(((o.area_ha - SUM(p.area_ha_planarized)) / o.area_ha * 100)::numeric, 2) as pct_diff
FROM overlap_summary o
INNER JOIN distinct_combinations p
ON o.designation && p.designation
GROUP BY o.designation, o.area_ha;


-- summarize the restrictions by type and level, and also compare the
-- source bc_boundary land area to the total area in the output _planarized table
-- (these should be very close to equal)
DROP TABLE IF EXISTS qa_summary;
CREATE TABLE qa_summary AS
SELECT * FROM
(SELECT
   1 as row,
  'Total area, BC' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM bc_boundary_land_tiled
UNION ALL
SELECT
  2 as row,
  'Total area, designations_planarized' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
UNION ALL
SELECT
3 as row,
  'Forest restricted, full' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE forest_restriction_max = 4
UNION ALL
SELECT
4 as row,
  'Forest restricted, high' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE forest_restriction_max = 3
UNION ALL
SELECT
5 as row,
  'Forest restricted, medium' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE forest_restriction_max = 2
UNION ALL
SELECT
6 as row,
  'Forest restricted, low' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE forest_restriction_max = 1
UNION ALL
SELECT
7 as row,
  'Forest restricted, none' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE forest_restriction_max = 0
UNION ALL
SELECT
8 as row,
  'Mine restricted, full' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE mine_restriction_max = 4
UNION ALL
SELECT
9 as row,
  'Mine restricted, high' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE mine_restriction_max = 3
UNION ALL
SELECT
10 as row,
  'Mine restricted, medium' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE mine_restriction_max = 2
UNION ALL
SELECT
11 as row,
  'Mine restricted, low' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE mine_restriction_max = 1
UNION ALL
SELECT
12 as row,
  'Mine restricted, none' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE mine_restriction_max = 0
UNION ALL
SELECT
13 as row,
  'Oil and Gas restricted, full' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE og_restriction_max = 4
UNION ALL
SELECT
14 as row,
  'Oil and Gas restricted, high' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE og_restriction_max = 3
UNION ALL
SELECT
15 as row,
  'Oil and Gas restricted, medium' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE og_restriction_max = 2
UNION ALL
SELECT
16 as row,
  'Oil and Gas restricted, low' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE og_restriction_max = 1
UNION ALL
SELECT
17 as row,
  'Oil and Gas restricted, none' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
WHERE og_restriction_max = 0
) as t
order by row;


-- Finally, add up the various restriction types - for each type, they should
-- add up to the total area of BC
DROP TABLE IF EXISTS qa_total_check;
CREATE TABLE qa_total_check AS
SELECT
  'Area total: designations_planarized' as description,
  ROUND((SUM(ST_Area(geom)) / 10000)::numeric) AS area_ha
FROM designations_planarized
UNION ALL
SELECT
'Area total: forest restrictions' as description,
  sum(area_ha)
FROM qa_summary
WHERE description LIKE 'Forest%%'
UNION ALL
SELECT
'Area total: mine restrictions' as description,
  sum(area_ha)
FROM qa_summary
WHERE description LIKE 'Mine%%'
UNION ALL
SELECT
'Area total: oil and gas restrictions' as description,
  sum(area_ha)
FROM qa_summary
WHERE description LIKE 'Oil%%';