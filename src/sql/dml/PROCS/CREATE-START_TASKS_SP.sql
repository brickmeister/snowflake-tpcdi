CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.START_TASKS_SP()
  returns string
  language javascript
  as
  $$
  // Purpose: this procedure is now defunct
  // Start LOAD_SNAPSHOT Task
  stmt = snowflake.createStatement(
      {sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.LOAD_SNAPSHOT_TSK "}
    );
  rs = stmt.execute();
  // Wait 10 Seconds
  stmt = snowflake.createStatement({sqlText:`call system$wait(10, 'SECONDS');`});
  rs = stmt.execute();
  // Start DIM_DATE_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_DATE_HISTORICAL_TSK "}
    );
  rs = stmt.execute();
 // Start DIM_TIME_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_TIME_HISTORICAL_TSK "}
    );
  rs = stmt.execute();
  // Start DIM_TRADE_TYPE_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_TRADE_TYPE_HISTORICAL_TSK "}
    );
  rs = stmt.execute();
  // Start DIM_STATUS_TYPE_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_STATUS_TYPE_HISTORICAL_TSK "}
    );
  rs = stmt.execute();
  // Start DIM_TAX_RATE_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_TAX_RATE_HISTORICAL_TSK "}
    );
  rs = stmt.execute();
  // Start DIM_INDUSTRY_HISTORICAL Task
  stmt = snowflake.createStatement(
      {sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_INDUSTRY_HISTORICAL_TSK "}
    );
  rs = stmt.execute();
  // Wait 30 Seconds
  stmt = snowflake.createStatement({sqlText:`call system$wait(30, 'SECONDS');`});
  rs = stmt.execute();
  // Wait 30 Seconds
  stmt = snowflake.createStatement({sqlText:`call system$wait(30, 'SECONDS');`});
  rs = stmt.execute();
  // Wait 60 Seconds
  stmt = snowflake.createStatement({sqlText:`call system$wait(60, 'SECONDS');`});
  rs = stmt.execute();
  // Start DIM_CUSTOMER Task
  stmt = snowflake.createStatement(
      {sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_CUSTOMER_INCREMENTAL_TSK "}
    );
  rs = stmt.execute();
  // Wait 10 Seconds
  stmt = snowflake.createStatement({sqlText:`call system$wait(10, 'SECONDS');`});
  rs = stmt.execute();
  rs.next();
  output = rs.getColumnValue(1);
  return output;
  $$
;
