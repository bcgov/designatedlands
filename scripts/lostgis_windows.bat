REM pgxnclient does not work on windows.
REM Rather than include required lostgis functions in our repository, use this
REM script to install them from source

REM ** modify your database connection parameters here **
SET PGHOST=localhost
SET PGPORT=5432
SET PGDATABASE=designatedlands
SET PGUSER=postgres

git clone https://github.com/gojuno/lostgis.git
psql -f lostgis\functions\ST_Safe_Difference.sql
psql -f lostgis\functions\ST_Safe_Intersection.sql
psql -f lostgis\functions\ST_Safe_Repair.sql