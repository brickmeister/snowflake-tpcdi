-- HISTORICAL LOAD

-- TRUNCATE TABLE
  TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_TRADE;
  COMMIT;

-- LOAD TABLE
  INSERT INTO TPCDI_WH.PUBLIC.DIM_TRADE
  SELECT
       COALESCE(TRADE_STG.T_ID,0) TRADE_ID
     , COALESCE(DIM_ACCOUNT.SK_BROKER_ID,0) SK_BROKER_ID
     , COALESCE(CASE WHEN (TH_ST_ID = 'SBMT' AND T_TT_ID IN ('TMB','TMS')) OR TH_ST_ID = 'PNDG' THEN DIM_DATE.DATE_ID END,0) SK_CREATE_DATE_ID
     , COALESCE(CASE WHEN (TH_ST_ID = 'SBMT' AND T_TT_ID IN ('TMB','TMS')) OR TH_ST_ID = 'PNDG' THEN DIM_TIME.TIME_ID END,0) SK_CREATE_TIME_ID
     , COALESCE(CASE WHEN TH_ST_ID IN ('CMPT','CNCL') THEN DIM_DATE.DATE_ID END,0) SK_CLOSE_DATE_ID
     , COALESCE(CASE WHEN TH_ST_ID IN ('CMPT','CNCL') THEN DIM_TIME.TIME_ID END,0) SK_CLOSE_TIME_ID
     , COALESCE(STATUSTYPE_STG.ST_NAME,'UNKNOWN') STATUS
     , COALESCE(TRADETYPE_STG.TT_NAME,'UNKNOWN') TYPE
     , TRADE_STG.T_IS_CASH CASH_FLAG
     , COALESCE(DIM_SECURITY.SK_SECURITY_ID,0) SK_SECURITY_ID
     , COALESCE(DIM_SECURITY.SK_COMPANY_ID,0) SK_COMPANY_ID
     , TRADE_STG.T_QTY QUANTITY
     , TRADE_STG.T_BID_PRICE BID_PRICE
     , COALESCE(DIM_ACCOUNT.SK_CUSTOMER_ID,0) SK_CUSTOMER_ID
     , COALESCE(DIM_ACCOUNT.SK_ACCOUNT_ID,0) SK_ACCOUNT_ID
     , TRADE_STG.T_EXEC_NAME EXECUTED_BY
     , TRADE_STG.T_TRADE_PRICE TRADE_PRICE
     , TRADE_STG.T_CHRG FEE
     , TRADE_STG.T_COMM COMMISSION
     , TRADE_STG.T_TAX TAX
     , TO_NUMBER(1) BATCH_ID
  FROM TPCDI_STG.PUBLIC.TRADE_STG
  LEFT JOIN TPCDI_STG.PUBLIC.TRADEHISTORY_STG ON
   	  TRADE_STG.T_ID = TRADEHISTORY_STG.TH_T_ID
  LEFT JOIN TPCDI_STG.PUBLIC.STATUSTYPE_STG ON
      TRADE_STG.T_ST_ID = STATUSTYPE_STG.ST_ID
  LEFT JOIN TPCDI_STG.PUBLIC.TRADETYPE_STG ON
      TRADE_STG.T_TT_ID = TRADETYPE_STG.TT_ID
  LEFT JOIN TPCDI_WH.PUBLIC.DIM_ACCOUNT ON
  	  TRADE_STG.T_CA_ID = DIM_ACCOUNT.ACCOUNT_ID
  LEFT JOIN TPCDI_WH.PUBLIC.DIM_SECURITY ON
  	  TRADE_STG.T_S_SYMB = DIM_SECURITY.SYMBOL
  LEFT JOIN TPCDI_WH.PUBLIC.DIM_DATE ON
  	  TO_DATE(TRADEHISTORY_STG.TH_DTS) = TO_DATE(DIM_DATE.DATE_VALUE)
  LEFT JOIN TPCDI_WH.PUBLIC.DIM_TIME ON
  	  TO_CHAR(TRADEHISTORY_STG.TH_DTS,'HH:MM:SS') = DIM_TIME.TIME_VALUE
  ;