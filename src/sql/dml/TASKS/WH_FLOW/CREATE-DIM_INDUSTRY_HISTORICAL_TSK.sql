CREATE OR REPLACE TASK TPCDI_WH.PUBLIC.DIM_INDUSTRY_HISTORICAL_TSK
  WAREHOUSE = TASK_WH_B
WHEN
  SYSTEM$STREAM_HAS_DATA('TPCDI_STG.PUBLIC.INDUSTRY_STG_STM')
AS
CALL TPCDI_WH.PUBLIC.DIM_INDUSTRY_HISTORICAL_SP()
;
