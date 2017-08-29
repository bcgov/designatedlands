-- Copyright 2017 Province of British Columbia
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
-- http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- 
-- See the License for the specific language governing permissions and limitations under the License.

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
                          --ST_Snaptogrid(geom_a, .001)
                          geom_a, geom_b, .1), 0)),
                 ST_Makevalid(
                    ST_Buffer(
                       --ST_Snaptogrid(geom_b, .001)
                       geom_b, 0)));
    EXCEPTION
        WHEN OTHERS THEN
            RETURN ST_GeomFromText('POLYGON EMPTY');
END;
$$ LANGUAGE 'plpgsql' STABLE STRICT;
