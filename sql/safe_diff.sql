-- With real world (messy) data, topology exceptions may occur when running
-- st_difference despite all the makevalids, snaptogrids and buffer 0s we
-- might throw at the geometry.
-- Catch and discard exceptions to allow processing to continue

--https://gis.stackexchange.com/questions/50399/how-best-to-fix-a-non-noded-intersection-problem-in-postgis

CREATE OR REPLACE FUNCTION safe_diff(geom_a geometry, geom_b geometry)
RETURNS geometry AS $$
BEGIN
    RETURN    ST_Difference(
                 ST_Makevalid(
                    ST_Buffer(
                       ST_Snap(
                          --ST_Snaptogrid(geom_a, .01)
                          geom_a, geom_b, .1), 0)),
                 ST_Makevalid(
                    ST_Buffer(
                       --ST_Snaptogrid(geom_b, .01)
                       geom_b, 0)));
    EXCEPTION
        WHEN OTHERS THEN
            RETURN ST_GeomFromText('POLYGON EMPTY');
END;
$$ LANGUAGE 'plpgsql' STABLE STRICT;