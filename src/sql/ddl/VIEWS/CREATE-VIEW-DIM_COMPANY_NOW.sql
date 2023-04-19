CREATE OR REPLACE VIEW TPCDI_WH.PUBLIC.DIM_COMPANY_NOW AS
WITH CURRENT_SK_IDS AS
(SELECT DISTINCT LAST_VALUE(SK_COMPANY_ID) OVER (PARTITION BY COMPANY_ID ORDER BY SK_COMPANY_ID) AS SK_COMPANY_ID_LAST
FROM TPCDI_WH.PUBLIC.DIM_COMPANY)
SELECT * FROM TPCDI_WH.PUBLIC.DIM_COMPANY
JOIN CURRENT_SK_IDS ON CURRENT_SK_IDS.SK_COMPANY_ID_LAST = DIM_COMPANY.SK_COMPANY_ID
;
