-- CREATE TABLE STATEMENT

CREATE OR REPLACE TABLE TPCDI_WH.PUBLIC.FACT_MARKET_HISTORY ( SK_SECURITY_ID NUMBER(11) NOT NULL COMMENT 'SURROGATE KEY FOR SECURITYID', SK_COMPANY_ID NUMBER(11) NOT NULL COMMENT 'SURROGATE KEY FOR COMPANYID', SK_DATE_ID NUMBER(11) NOT NULL COMMENT 'SURROGATE KEY FOR THE DATE', PE_RATIO NUMBER(10,2) COMMENT 'PRICE TO EARNINGS PER SHARE RATIO', YIELD NUMBER(5,2) NOT NULL COMMENT 'DIVIDEND TO PRICE RATIO, AS A PERCENTAGE', FIFTY_TWO_WEEK_HIGH NUMBER(8,2) NOT NULL COMMENT 'SECURITY HIGHEST PRICE IN LAST 52 WEEKS FROM THIS DAY', SK_FIFTY_TWO_WEEK_HIGH_DATE NUMBER(11) NOT NULL COMMENT 'EARLIEST DATE ON WHICH THE 52 WEEK HIGH PRICE WAS SET', FIFTY_TWO_WEEK_LOW NUMBER(8,2) NOT NULL COMMENT 'SECURITY LOWEST PRICE IN LAST 52 WEEKS FROM THIS DAY', SK_FIFTY_TWO_WEEK_LOW_DATE NUMBER(11) NOT NULL COMMENT 'EARLIEST DATE ON WHICH THE 52 WEEK LOW PRICE WAS SET', CLOSE_PRICE NUMBER(8,2) NOT NULL COMMENT 'SECURITY CLOSING PRICE ON THIS DAY', DAY_HIGH NUMBER(8,2) NOT NULL COMMENT 'HIGHEST PRICE FOR THE SECURITY ON THIS DAY', DAY_LOW NUMBER(8,2) NOT NULL COMMENT 'LOWEST PRICE FOR THE SECURITY ON THIS DAY', VOLUME NUMBER(12) NOT NULL COMMENT 'TRADING VOLUME OF THE SECURITY ON THIS DAY', BATCH_ID NUMBER(5) NOT NULL COMMENT 'BATCH ID WHEN THIS RECORD WAS INSERTED' ) ;
