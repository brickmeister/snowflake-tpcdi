CREATE OR REPLACE VIEW TPCDI_WH.PUBLIC.DIM_FINANCIAL_ROLL_YEAR_EPS AS
  SELECT
  DIM_COMPANY.SK_COMPANY_ID,
  DIM_COMPANY.COMPANY_ID,
  DIM_FINANCIAL.FI_QTR_START_DATE,
  YEAR(DIM_FINANCIAL.FI_QTR_START_DATE)::STRING || QUARTER(DIM_FINANCIAL.FI_QTR_START_DATE)::STRING AS YEAR_QTR,
  DIM_FINANCIAL.FI_BASIC_EPS,
  SUM(DIM_FINANCIAL.FI_BASIC_EPS) OVER (PARTITION BY DIM_FINANCIAL.SK_COMPANYID ORDER BY DIM_FINANCIAL.FI_QTR_START_DATE ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ROLL_YEAR_EPS
  FROM TPCDI_WH.PUBLIC.DIM_FINANCIAL
  INNER JOIN TPCDI_WH.PUBLIC.DIM_COMPANY ON
    DIM_FINANCIAL.SK_COMPANYID = DIM_COMPANY.SK_COMPANY_ID;

CREATE OR REPLACE VIEW TPCDI_WH.PUBLIC.DIM_ACCOUNT_NOW AS
  WITH CURRENT_SK_IDS AS
  (SELECT DISTINCT LAST_VALUE(SK_ACCOUNT_ID) OVER (PARTITION BY ACCOUNT_ID ORDER BY SK_ACCOUNT_ID) AS SK_ACCOUNT_ID_LAST
  FROM TPCDI_WH.PUBLIC.DIM_ACCOUNT)
  SELECT * FROM TPCDI_WH.PUBLIC.DIM_ACCOUNT
  JOIN CURRENT_SK_IDS ON CURRENT_SK_IDS.SK_ACCOUNT_ID_LAST = DIM_ACCOUNT.SK_ACCOUNT_ID;

CREATE OR REPLACE VIEW TPCDI_WH.PUBLIC.DIM_COMPANY_NOW AS
  WITH CURRENT_SK_IDS AS
  (SELECT DISTINCT LAST_VALUE(SK_COMPANY_ID) OVER (PARTITION BY COMPANY_ID ORDER BY SK_COMPANY_ID) AS SK_COMPANY_ID_LAST
  FROM TPCDI_WH.PUBLIC.DIM_COMPANY)
  SELECT * FROM TPCDI_WH.PUBLIC.DIM_COMPANY
  JOIN CURRENT_SK_IDS ON CURRENT_SK_IDS.SK_COMPANY_ID_LAST = DIM_COMPANY.SK_COMPANY_ID;

CREATE OR REPLACE VIEW TPCDI_WH.PUBLIC.DIM_CUSTOMER_NOW AS
  WITH CURRENT_SK_IDS AS
  (SELECT DISTINCT LAST_VALUE(SK_CUSTOMER_ID) OVER (PARTITION BY CUSTOMER_ID ORDER BY SK_CUSTOMER_ID) AS SK_CUSTOMER_ID_LAST
  FROM TPCDI_WH.PUBLIC.DIM_CUSTOMER)
  SELECT * FROM TPCDI_WH.PUBLIC.DIM_CUSTOMER
  JOIN CURRENT_SK_IDS ON CURRENT_SK_IDS.SK_CUSTOMER_ID_LAST = DIM_CUSTOMER.SK_CUSTOMER_ID;

CREATE OR REPLACE VIEW TPCDI_WH.PUBLIC.DIM_SECURITY_NOW AS
  WITH CURRENT_SK_IDS AS
  (SELECT DISTINCT LAST_VALUE(SK_SECURITY_ID) OVER (PARTITION BY SYMBOL ORDER BY SK_SECURITY_ID) AS SK_SECURITY_ID_LAST
  FROM TPCDI_WH.PUBLIC.DIM_SECURITY)
  SELECT * FROM TPCDI_WH.PUBLIC.DIM_SECURITY
  JOIN CURRENT_SK_IDS ON CURRENT_SK_IDS.SK_SECURITY_ID_LAST = DIM_SECURITY.SK_SECURITY_ID;
  
CREATE OR REPLACE VIEW TPCDI_WH.PUBLIC.TABLE_ROW_COUNTS AS
  SELECT 
  TABLE_CATALOG AS DATABASE,
  TABLE_NAME, 
  ROW_COUNT
  FROM TPCDI_WH.INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA = 'PUBLIC'
  UNION
  SELECT 
  TABLE_CATALOG AS DATABASE,
  TABLE_NAME, 
  ROW_COUNT
  FROM TPCDI_ODS.INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA = 'PUBLIC'
  UNION
  SELECT 
  TABLE_CATALOG AS DATABASE,
  TABLE_NAME, 
  ROW_COUNT
  FROM TPCDI_STG.INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA = 'PUBLIC';

CREATE OR REPLACE VIEW TPCDI_STG.PUBLIC.DAILY_MARKET_HIGH_LOW_STG AS
  WITH GET_HIGHS_LOWS AS (
  SELECT
  DM_S_SYMB SYMBOL,
  DIM_DATE.DATE_ID DATE,
  DM_HIGH DAY_HIGH,
  DM_LOW DAY_LOW,
  MAX(DM_HIGH) OVER (PARTITION BY DM_S_SYMB ORDER BY DM_DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS YEAR_HIGH,
  MIN(DM_LOW) OVER (PARTITION BY DM_S_SYMB ORDER BY DM_DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) AS YEAR_LOW
  FROM TPCDI_STG.PUBLIC.DAILYMARKET_STG
  INNER JOIN TPCDI_WH.PUBLIC.DIM_DATE ON
    DAILYMARKET_STG.DM_DATE = DIM_DATE.DATE_VALUE
  ORDER BY 1,2
  )
  SELECT
  GET_HIGHS_LOWS.SYMBOL,
  GET_HIGHS_LOWS.DATE,
  GET_HIGHS_LOWS.DAY_HIGH,
  GET_HIGHS_LOWS.YEAR_HIGH,
  REPLACE(MIN(HIGH.DM_DATE)::STRING,'-')::NUMBER AS EARLIEST_HIGH_DATE,
  GET_HIGHS_LOWS.DAY_LOW,
  GET_HIGHS_LOWS.YEAR_LOW,
  REPLACE(MIN(LOW.DM_DATE)::STRING,'-')::NUMBER AS EARLIEST_LOW_DATE
  FROM GET_HIGHS_LOWS
  INNER JOIN TPCDI_STG.PUBLIC.DAILYMARKET_STG HIGH
      ON HIGH.DM_S_SYMB = GET_HIGHS_LOWS.SYMBOL AND HIGH.DM_HIGH = GET_HIGHS_LOWS.YEAR_HIGH
  INNER JOIN TPCDI_STG.PUBLIC.DAILYMARKET_STG LOW
      ON LOW.DM_S_SYMB = GET_HIGHS_LOWS.SYMBOL AND LOW.DM_LOW = GET_HIGHS_LOWS.YEAR_LOW
  GROUP BY 1,2,3,4,6,7
  ORDER BY 1,2;