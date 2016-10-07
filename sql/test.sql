-- prep / clean
DROP TABLE IF EXISTS temp.test_uwr_tile_bc;
CREATE TABLE temp.test_uwr_tile_bc AS

SELECT 'uwr_no_harvest'::TEXT as category,
  st_subdivide((st_dump(
      ST_Union(
        ST_Multi(
          ST_Buffer(
            st_snaptogrid(a.geom, .001), 0))))).geom) as geom
FROM conservation_lands.uwr_no_harvest a
GROUP BY category;

-- index
CREATE INDEX uwr_gix ON temp.test_uwr_tile_bc USING GIST (geom);

-- get data
DROP TABLE IF EXISTS temp.test_uwr_094C;
CREATE TABLE temp.test_uwr_094C AS
SELECT
    category,
    CASE
     WHEN ST_Within(a.geom, grd.geom) THEN a.geom
     ELSE ST_Intersection(ST_MakeValid(a.geom), ST_MakeValid(grd.geom))
    END as geom
FROM
  conservation_lands.uwr_no_harvest a,
  whse_basemapping.nts_250k_grid grd
WHERE
  ST_Intersects(a.geom, grd.geom) AND a.geom && grd.geom
AND grd.map_tile = '094C';