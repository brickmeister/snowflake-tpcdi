CREATE OR REPLACE TASK TPCDI_WH.PUBLIC.DIM_SECURITY_HISTORICAL_TSK
  WAREHOUSE = TASK_WH_A
  AFTER TPCDI_WH.PUBLIC.DIM_COMPANY_HISTORICAL_TSK
AS
CALL TPCDI_WH.PUBLIC.DIM_SECURITY_HISTORICAL_SP()
;