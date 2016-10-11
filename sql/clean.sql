-- Clean input data

-- create empty table with new auto-indexed id column
CREATE UNLOGGED TABLE IF NOT EXISTS $out_table (
     id serial PRIMARY KEY,
     category text,
     geom geometry
);

-- insert cleaned data
INSERT INTO $out_table (category, geom)
  SELECT category, geom
  FROM (SELECT
          '$layer'::TEXT as category,
  -- subdivide for speed
          st_subdivide((st_dump(
  -- union to remove overlapping polys within the source
            ST_Union(
  -- make buffer multipart
                ST_Multi(
  -- buffer the features by 0 to make sure they are vaild
                  ST_Buffer(
  -- snap to 1mm grid just to keep things simple and hopefully remove
  -- some precision errors/slivers
                    st_snaptogrid(a.geom, .001), 0))))).geom) as geom
        FROM $source a
        GROUP BY category) as foo;

-- index for speed
CREATE INDEX $layer_gix ON $out_table USING GIST (geom);
