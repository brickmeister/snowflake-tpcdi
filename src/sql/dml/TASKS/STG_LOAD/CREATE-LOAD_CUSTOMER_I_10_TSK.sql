CREATE OR REPLACE TASK TPCDI_STG.PUBLIC.LOAD_CUSTOMER_I_10_TSK
  WAREHOUSE = TPCDI_FILE_LOAD
  
AS
CALL TPCDI_STG.PUBLIC.LOAD_CUSTOMER_SP(10,3,60)
;
