CREATE OR REPLACE TASK TPCDI_WH.PUBLIC.DIM_CUSTOMER_HISTORICAL_TSK
  WAREHOUSE = TASK_WH_A
  AFTER TPCDI_WH.PUBLIC.DIM_REFERENCE_HISTORICAL_TSK
AS
CALL TPCDI_WH.PUBLIC.DIM_CUSTOMER_HISTORICAL_SP()
;
