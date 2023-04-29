CREATE OR REPLACE PROCEDURE TPCDI_WH.PUBLIC.START_DW_HISTORICAL_TASKS_SP()
  returns string
  language javascript
  as
  $$
  // Purpose:  all tasks associated with processing the historical data into the tpcdi_wh database
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_REFERENCE_HISTORICAL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_CUSTOMER_HISTORICAL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_COMPANY_HISTORICAL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_ACCOUNT_HISTORICAL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_SECURITY_HISTORICAL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_FINANCIAL_HISTORICAL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.CHECK_STREAM_CTRL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.DIM_TRADE_HISTORICAL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.FACT_CASH_BALANCES_HISTORICAL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY_HISTORICAL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.FACT_PROSPECT_HISTORICAL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.FACT_HOLDINGS_HISTORICAL_TSK "});
  stmt.execute();
  var stmt = snowflake.createStatement({sqlText: "EXECUTE TASK TPCDI_WH.PUBLIC.FACT_WATCHES_HISTORICAL_TSK "});
  stmt.execute();
  return "All historical DW tasks started.";
  $$
;
