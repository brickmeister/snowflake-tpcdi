CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_TIME_SP(scale float)
  returns string
  language javascript
  as
  $$
  var tpcdi_scale = SCALE
  // Load TIME_STG
  stmt = snowflake.createStatement(
      {sqlText: "COPY INTO TPCDI_STG.PUBLIC.TIME_STG FROM @TPCDI_FILES//tpcdi/sf=" + tpcdi_scale + "/Batch1/Time.txt FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
    );
  rs = stmt.execute();
  rs.next();
  // Stop task
  var stoptask_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_TIME_" + tpcdi_scale + "_TSK SUSPEND"});
  stoptask_stmt.execute();
  return "All time files have been loaded.";
  $$
;
