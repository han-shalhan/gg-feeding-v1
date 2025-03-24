-----------------------------------------------------  [ PROCESSING ] --------------------------------------------------------------------

-- Define the procedure with dynamic parameters
CREATE OR REPLACE PROCEDURE `gg-gcp.temp_dataset.home_residence_processing` (
  input_traffic_boundary STRING,
  raw_h3_output STRING,
  intermediate_result_table STRING,
  output_deduplicated_table STRING,
  output_clean_table STRING,
  min_total_num_visit INT64,
  original_traffic STRING,
  start_date DATE,
  end_date DATE,
  append_residence_loc STRING
)
BEGIN
  -- Declare SQL query variables
  DECLARE h3_segment_query STRING;
  DECLARE agg_and_filter_query STRING;
  DECLARE remove_duplicate_query STRING;
  DECLARE generate_h3_center_query STRING;
  DECLARE join_residence STRING;
  DECLARE summary_query STRING;
  DECLARE delete_temp STRING;

  -- H3 Segment Calculation
  SET h3_segment_query = '''
    CREATE OR REPLACE TABLE `''' || raw_h3_output || '''` AS
    WITH T1 AS (
      SELECT ifa,
             h3,
             COUNT(DISTINCT DATE(TIMESTAMP(timestamp))) AS num_visit_month
      FROM `''' || input_traffic_boundary || '''`
      WHERE timestamp IS NOT NULL
      GROUP BY ifa, h3
    )
    SELECT ifa, h3, num_visit_month
    FROM T1
    ORDER BY ifa, num_visit_month DESC
  ''';

  -- Aggregate and Filter
  SET agg_and_filter_query = '''
    CREATE OR REPLACE TABLE `''' || intermediate_result_table || '''` AS
    WITH SumVisits AS (
      SELECT ifa,
             SUM(num_visit_month) AS total_num_visit
      FROM `''' || raw_h3_output || '''`
      GROUP BY ifa
    ),
    Percentages AS (
      SELECT t.ifa,
             t.h3,
             t.num_visit_month,
             s.total_num_visit,
             ROUND(t.num_visit_month / s.total_num_visit, 4) AS percentage,
             RANK() OVER (PARTITION BY t.ifa ORDER BY (t.num_visit_month / s.total_num_visit) DESC) AS ranked
      FROM `''' || raw_h3_output || '''` t
      JOIN SumVisits s ON t.ifa = s.ifa
    )
    SELECT *, ROW_NUMBER() OVER(PARTITION BY ifa ORDER BY h3) AS h3_filter
    FROM Percentages
    WHERE num_visit_month >= ''' || CAST(min_total_num_visit AS STRING) || '''
          AND ranked = 1
    ORDER BY ifa, ranked, h3_filter
  ''';

  -- Remove Duplicates
  SET remove_duplicate_query = '''
    CREATE OR REPLACE TABLE `''' || output_deduplicated_table || '''` AS
    SELECT * EXCEPT(ranked, h3_filter)
    FROM `''' || intermediate_result_table || '''`
    WHERE h3_filter = 1
  ''';

  -- Generate H3 Center
  SET generate_h3_center_query = '''
    CREATE OR REPLACE TABLE `''' || output_clean_table || '''` AS
    SELECT *,
           `carto-un-as-se2`.carto.H3_CENTER(h3) AS residence_location
    FROM `''' || output_deduplicated_table || '''`
    ORDER BY ifa
  ''';

  -- Join residence location into traffic table
  SET join_residence = '''
      CREATE OR REPLACE TABLE `''' || append_residence_loc || '''` AS
      SELECT p.*, po.residence_location
      FROM `''' || original_traffic || '''` AS p
      LEFT JOIN `''' || output_clean_table || '''` AS po
      ON p.ifa = po.ifa
      WHERE timestamp >= \'''' || FORMAT_TIMESTAMP('%Y-%m-%d', start_date) || '''\'
      AND timestamp < \'''' || FORMAT_TIMESTAMP('%Y-%m-%d', end_date) || '''\'
  ''';


  -- Summary Query for Result Validation
  SET summary_query = '''
    SELECT
      (SELECT COUNT(DISTINCT ifa) FROM `''' || raw_h3_output || '''`) AS raw_count,
      (SELECT COUNT(DISTINCT ifa) FROM `''' || output_clean_table || '''`) AS clean_count,
      ROUND((SELECT COUNT(DISTINCT ifa) FROM `''' || output_clean_table || '''`)
            / (SELECT COUNT(DISTINCT ifa) FROM `''' || raw_h3_output || '''`) * 100, 2) AS percentage_identified
  ''';

  -- Delete Temp Table
  SET delete_temp = '''
    DROP TABLE `''' || input_traffic_boundary || '''`
    DROP TABLE `''' || raw_h3_output || '''`
    DROP TABLE `''' || intermediate_result_table || '''`
    DROP TABLE `''' || output_deduplicated_table || '''`
    DROP TABLE `''' || output_clean_table || '''`
  ''';

  -- Execute the dynamic SQL queries
  EXECUTE IMMEDIATE h3_segment_query;
  EXECUTE IMMEDIATE agg_and_filter_query;
  EXECUTE IMMEDIATE remove_duplicate_query;
  EXECUTE IMMEDIATE generate_h3_center_query;
  EXECUTE IMMEDIATE join_residence;

  -- -- Output summary for validation
  EXECUTE IMMEDIATE summary_query;

  -- Delete Temp
  --EXECUTE IMMEDIATE delete_temp;

END;