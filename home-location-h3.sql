-----------------------------------------------------  [ PREPROCESSING ] --------------------------------------------------------------------
-- PROCEDURE DEFINITION
CREATE OR REPLACE PROCEDURE `gg-gcp.temp_dataset.home_residence_preprocess`(
  input_traffic_table STRING,
  output_traffic_table STRING,
  start_date DATE,
  end_date DATE,
  hours ARRAY<INT64>,
  refference_h3_boundary STRING,
  geom_h3_boundary STRING,
  append_traffic_boundary STRING
)

BEGIN
  -- Declare variables at the start of the block
  DECLARE traffic_sql_query STRING;
  DECLARE hours_filter STRING;
  DECLARE match_traffic_boundary STRING;
  DECLARE h3_boundary_preprocess STRING;

  -- Construct the hours filter
  SET hours_filter = ARRAY_TO_STRING(ARRAY(
    SELECT CAST(hour AS STRING)
    FROM UNNEST(hours) AS hour
  ), ', ');

  -- Construct the SQL query for traffic data
  SET traffic_sql_query = 'CREATE OR REPLACE TABLE `' || output_traffic_table || '` AS '
                          || 'SELECT *'
                          || 'FROM (SELECT ifa, timestamp, geom FROM `' || input_traffic_table || '` )'
                          || 'WHERE TIMESTAMP >= "' || FORMAT_TIMESTAMP('%Y-%m-%d', start_date) || '" '
                          || 'AND TIMESTAMP < "' || FORMAT_TIMESTAMP('%Y-%m-%d', end_date) || '" '
                          || 'AND EXTRACT(HOUR FROM TIMESTAMP) IN (' || hours_filter || ')';

  SET h3_boundary_preprocess = '''
                               CREATE OR REPLACE TABLE `''' || geom_h3_boundary || '''` AS
                               SELECT *, `carto-un-as-se2`.carto.H3_BOUNDARY(h3) AS geom
                               FROM `''' || refference_h3_boundary || '''`
                               ''';


  -- Match Traffic and Boundary Query
  SET match_traffic_boundary = 
      'CREATE OR REPLACE TABLE `' || append_traffic_boundary || '` AS ' ||
      'SELECT p.ifa, p.timestamp, p.geom, po.h3 ' ||
      'FROM (SELECT * FROM `' || output_traffic_table || '`) AS p ' ||
      'JOIN (SELECT *,  FROM `' || geom_h3_boundary || '`) AS po ' ||
      'ON ST_INTERSECTS(p.geom, po.geom)';


  -- Execute the dynamic SQL queries
  EXECUTE IMMEDIATE traffic_sql_query;
  EXECUTE IMMEDIATE h3_boundary_preprocess;
  EXECUTE IMMEDIATE match_traffic_boundary;

END;