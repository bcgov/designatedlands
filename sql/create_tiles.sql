-- Some marine portions of input data falls outside of the 20k tiles (such as
-- Conservancies) To fill the gaps, create a tile layer that includes 250k
-- tiles for marine areas (except for SSOG to avoid confusion with USA border)

DROP TABLE IF EXISTS tiles;
CREATE TABLE tiles (tile_id serial primary key, map_tile text, geom geometry);

CREATE INDEX ON tiles (map_tile);
CREATE INDEX tiles_gidx ON tiles USING GIST (geom) ;

INSERT INTO tiles (map_tile, geom)
SELECT map_tile, geom FROM tiles_20k;

INSERT INTO tiles (map_tile, geom)
SELECT a.map_tile, st_difference(a.geom, st_union(b.geom))
FROM tiles_250k a
INNER JOIN tiles_20k b
ON ST_Intersects(a.geom, b.geom)
WHERE a.map_tile IN
  ('092E','092C','102I','102O','102P','103A','103B',
   '103F','103G', '103H', '103C','103J','103K')
GROUP BY a.map_tile, a.geom;
