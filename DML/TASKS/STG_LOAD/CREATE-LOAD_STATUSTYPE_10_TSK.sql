CREATE OR REPLACE TASK TPCDI_STG.PUBLIC.LOAD_STATUSTYPE_10_TSK
  WAREHOUSE = TPCDI_FILE_LOAD,
  SCHEDULE = '10 SECOND'
AS
CALL TPCDI_STG.PUBLIC.LOAD_STATUSTYPE_SP(10)
;
