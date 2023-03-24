-- INCREMENTAL LOAD

-- ALTER SESSION TO SET PARAMETER TO ALLOW MULTIPLE INSTANCES OF SAME BUSINESS ID FOR UPDATES IN STAGE
ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = FALSE;

-- MERGE RECORDS FROM STAGE
MERGE INTO TPCDI_WH.PUBLIC.DIM_SECURITY USING
    (
	  SELECT
			 SYMBOL SYMBOL
		   , ISSUE_TYPE ISSUE
		   , STATUSTYPE_STG.ST_NAME STATUS
		   , FINWIRE_SEC_STG.NAME NAME
		   , EX_ID EXCHANGE_ID
		   , COALESCE(COMP1.SK_COMPANY_ID,COMP2.SK_COMPANY_ID,0) SK_COMPANY_ID
		   , SH_OUT SHARES_OUTSTANDING
		   , FIRST_TRADE_DATE FIRST_TRADE
		   , FIRST_TRADE_EXCHG FIRST_TRADE_ON_EXCHANGE
		   , DIVIDEND DIVIDEND
		   , TO_NUMBER(1) IS_CURRENT
		   , TO_NUMBER(2) BATCH_ID
		   , EFFECTIVE_DATE.EFFECTIVE_DATE EFFECTIVE_DATE
		   , TO_DATE('12/31/9999','MM/DD/YYYY') END_DATE
	  FROM TPCDI_STG.PUBLIC.FINWIRE_SEC_STG
	  JOIN 
		  (SELECT
			  MIN(DATE_VALUE) EFFECTIVE_DATE
		   FROM TPCDI_WH.PUBLIC.DIM_DATE) EFFECTIVE_DATE ON
		  1 = 1
	  LEFT JOIN TPCDI_STG.PUBLIC.STATUSTYPE_STG ON
		  FINWIRE_SEC_STG.STATUS = STATUSTYPE_STG.ST_ID
	  LEFT JOIN TPCDI_WH.PUBLIC.DIM_COMPANY COMP1 ON
		  FINWIRE_SEC_STG.CO_NAME_OR_CIK = COMP1.NAME
	  LEFT JOIN TPCDI_WH.PUBLIC.DIM_COMPANY COMP2 ON
		  FINWIRE_SEC_STG.CO_NAME_OR_CIK = TO_CHAR(COMP2.COMPANY_ID)
    ) SECURITY_UPDATES ON DIM_SECURITY.SYMBOL = SECURITY_UPDATES.SYMBOL
WHEN MATCHED THEN UPDATE SET
    DIM_SECURITY.SYMBOL = COALESCE(SECURITY_UPDATES.SYMBOL,DIM_SECURITY.SYMBOL),
    DIM_SECURITY.ISSUE = COALESCE(SECURITY_UPDATES.ISSUE,DIM_SECURITY.ISSUE),
    DIM_SECURITY.STATUS = COALESCE(SECURITY_UPDATES.STATUS,DIM_SECURITY.STATUS),
    DIM_SECURITY.NAME = COALESCE(SECURITY_UPDATES.NAME,DIM_SECURITY.NAME),
    DIM_SECURITY.EXCHANGE_ID = COALESCE(SECURITY_UPDATES.EXCHANGE_ID,DIM_SECURITY.EXCHANGE_ID),
    DIM_SECURITY.SK_COMPANY_ID = COALESCE(SECURITY_UPDATES.SK_COMPANY_ID,DIM_SECURITY.SK_COMPANY_ID),
    DIM_SECURITY.SHARES_OUTSTANDING = COALESCE(SECURITY_UPDATES.SHARES_OUTSTANDING,DIM_SECURITY.SHARES_OUTSTANDING),
    DIM_SECURITY.FIRST_TRADE = COALESCE(SECURITY_UPDATES.FIRST_TRADE,DIM_SECURITY.FIRST_TRADE),
    DIM_SECURITY.FIRST_TRADE_ON_EXCHANGE = COALESCE(SECURITY_UPDATES.FIRST_TRADE_ON_EXCHANGE,DIM_SECURITY.FIRST_TRADE_ON_EXCHANGE),
    DIM_SECURITY.DIVIDEND = COALESCE(SECURITY_UPDATES.DIVIDEND,DIM_SECURITY.DIVIDEND),
    DIM_SECURITY.IS_CURRENT = COALESCE(SECURITY_UPDATES.IS_CURRENT,DIM_SECURITY.IS_CURRENT),
    DIM_SECURITY.BATCH_ID = COALESCE(SECURITY_UPDATES.BATCH_ID,DIM_SECURITY.BATCH_ID),
    DIM_SECURITY.EFFECTIVE_DATE = COALESCE(SECURITY_UPDATES.EFFECTIVE_DATE,DIM_SECURITY.EFFECTIVE_DATE),
    DIM_SECURITY.END_DATE = COALESCE(SECURITY_UPDATES.END_DATE,DIM_SECURITY.END_DATE)
WHEN NOT MATCHED THEN INSERT
    ( SK_SECURITY_ID,
      SYMBOL,
      ISSUE,
      STATUS,
      NAME,
      EXCHANGE_ID,
      SK_COMPANY_ID,
      SHARES_OUTSTANDING,
      FIRST_TRADE,
      FIRST_TRADE_ON_EXCHANGE,
      DIVIDEND,
      IS_CURRENT,
      BATCH_ID,
      EFFECTIVE_DATE,
      END_DATE )
VALUES
    ( TPCDI_WH.PUBLIC.DIM_SECURITY_SEQ.NEXTVAL,
      SECURITY_UPDATES.SYMBOL,
      SECURITY_UPDATES.ISSUE,
      SECURITY_UPDATES.STATUS,
      SECURITY_UPDATES.NAME,
      SECURITY_UPDATES.EXCHANGE_ID,
      SECURITY_UPDATES.SK_COMPANY_ID,
      SECURITY_UPDATES.SHARES_OUTSTANDING,
      SECURITY_UPDATES.FIRST_TRADE,
      SECURITY_UPDATES.FIRST_TRADE_ON_EXCHANGE,
      SECURITY_UPDATES.DIVIDEND,
      SECURITY_UPDATES.IS_CURRENT,
      SECURITY_UPDATES.BATCH_ID,
      SECURITY_UPDATES.EFFECTIVE_DATE,
      SECURITY_UPDATES.END_DATE
    )
;