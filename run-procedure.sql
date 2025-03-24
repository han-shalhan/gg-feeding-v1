-----------------------------------------------------  [ EXECUTE PROCEDURE ] --------------------------------------------------------------------

-- EXECUTE LOCAL TIMEZONE
CALL `gg-gcp.temp_dataset.timezone_preprocess`(
  'gg-dataset.mw_dataset.mob_feb_25', --- Input Table for Traffic (change this table as traffic month)
  'gg-gcp.data_refference.time_zone_boundary_indo', --- Output Table for Traffic (change this table as traffic month)
  'gg-dataset.temp_dataset.mob_feb_25_ready' -- Append Table (CREATE PARTITION TABLE FIRST!!) (change this table as traffic month)
);


-- EXECUTE PROCEDURE
CALL `gg-gcp.temp_dataset.home_residence_preprocess`(
  'gg-dataset.temp_dataset.mob_feb_25_ready', --- Input Table for Traffic (change this table as traffic month)
  'gg-dataset.temp_dataset.mw_trim_feb25', --- Output Table for Traffic (change this table as traffic month)
  DATE '2025-02-01', -- Start Date Filter for Traffic (adjust on traffic month)
  DATE '2025-03-01', -- End Date Filter for Traffic (adjust on traffic month)
  [20, 21, 22, 23, 0, 1, 2, 3, 4, 5, 6, 7, 8], -- Timestamp filter for Traffic
  'gg-gcp.data_refference.h3_res8_master_prov', -- Input H3 Boundary Table (keep same)
  'gg-gcp.data_refference.h3_res8_master_prov_geom',
  'gg-dataset.temp_dataset.mw_trim_feb25_h3' -- Append Table (CREATE PARTITION TABLE FIRST!!) (change this table as traffic month)
);


-- Execute the procedure with dynamic parameters
CALL `gg-gcp.temp_dataset.home_residence_processing`(
  'gg-dataset.temp_dataset.mw_trim_feb25_h3',    -- Input Traffic Boundary Table (based on matching traffic + h3 table)
  'gg-dataset.temp_dataset.temp0_mw_h3_rnd',       -- Output Raw Table (ignore)
  'gg-dataset.temp_dataset.temp0_h3_result_rnd',    -- Intermediate Result Table (ignore)
  'gg-dataset.temp_dataset.temp0_h3_unq_rnd',     -- Output Deduplicated Table (ignore)
  'gg-dataset.temp_dataset.temp0_h3_clean',       -- Output Clean Table (ignore, this is final temporary table)
  5,                                         -- Minimum Total Number of Visits
  'gg-dataset.temp_dataset.mob_feb_25_ready',         -- Original traffic table (adjust with ur main traffic table)
  DATE '2025-02-01', -- Start Date Filter for Traffic
  DATE '2025-03-01', -- End Date Filter for Traffic
  'gg-dataset.mw_dataset.mob_feb_25_final' --  residence location final result
);


-----------------------------------------------------  [ END OF PROCEDURE ] --------------------------------------------------------------------