CREATE OR REPLACE TASK TPCDI_STG.PUBLIC.LOAD_TRADE_I_10_TSK
  WAREHOUSE = TPCDI_FILE_LOAD,
  SCHEDULE = '10 SECOND'
AS
CALL TPCDI_STG.PUBLIC.LOAD_TRADE_I_SP(10,3,60)
;
