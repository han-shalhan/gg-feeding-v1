-----------------------------------------------------  [ TIMEZONE CONVERSION ] --------------------------------------------------------------------
-- PROCEDURE DEFINITION
CREATE OR REPLACE PROCEDURE `gg-gcp.temp_dataset.timezone_preprocess`(
  input_raw_mobility STRING,
  input_timezone_refference STRING,
  mw_local_time STRING
)

BEGIN
  -- Declare variables at the start of the block
  DECLARE convert_timezone STRING;

  -- SQL Query Timezone
  SET convert_timezone = '''
                        CREATE OR REPLACE TABLE `''' || mw_local_time || '''` AS 
                              SELECT * EXCEPT(geom, geoms, timestamp),
                                    geoms AS geom,
                                    CASE 
                                        WHEN time_zone = 'WIB' THEN DATETIME(timestamp, "Asia/Jakarta")
                                        WHEN time_zone = 'WITA' THEN DATETIME(timestamp, "Asia/Makassar")
                                        WHEN time_zone = 'WIT' THEN DATETIME(timestamp, "Asia/Jayapura")
                                        ELSE NULL
                                    END AS timestamp
                              FROM (
                                    SELECT *, ST_GEOGPOINT(longitude, latitude) AS geoms 
                                    FROM `''' || input_raw_mobility || '''`
                                    ) A
                              JOIN `''' || input_timezone_refference || '''` B
                              ON ST_INTERSECTS(A.geoms, B.geom);
                               ''';

  -- Execute the dynamic SQL queries
  EXECUTE IMMEDIATE convert_timezone;

END;