CREATE OR REPLACE VIEW TPCDI_WH.PUBLIC.DIM_ACCOUNT_NOW AS
WITH CURRENT_SK_IDS AS
(SELECT DISTINCT LAST_VALUE(SK_ACCOUNT_ID) OVER (PARTITION BY ACCOUNT_ID ORDER BY SK_ACCOUNT_ID) AS SK_ACCOUNT_ID_LAST
FROM TPCDI_WH.PUBLIC.DIM_ACCOUNT)
SELECT * FROM TPCDI_WH.PUBLIC.DIM_ACCOUNT
JOIN CURRENT_SK_IDS ON CURRENT_SK_IDS.SK_ACCOUNT_ID_LAST = DIM_ACCOUNT.SK_ACCOUNT_ID
;
