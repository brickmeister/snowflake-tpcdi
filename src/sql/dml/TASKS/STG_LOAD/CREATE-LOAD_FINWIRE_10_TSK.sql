
CREATE OR REPLACE TASK TPCDI_STG.PUBLIC.LOAD_FINWIRE_10_TSK
  WAREHOUSE = TPCDI_FILE_LOAD
  
AS
CALL TPCDI_STG.PUBLIC.LOAD_FINWIRE_SP(10)
;
