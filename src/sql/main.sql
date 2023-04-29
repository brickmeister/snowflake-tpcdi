-- RESET TABLES, SEQUENCES, AND STREAMS

TRUNCATE TABLE TPCDI_WH.PUBLIC.LOAD_SNAPSHOT;

-- TRUNCATE STAGE TABLES
TRUNCATE TABLE TPCDI_STG.PUBLIC.DATE_STG;
TRUNCATE TABLE TPCDI_STG.PUBLIC.TIME_STG;
TRUNCATE TABLE TPCDI_STG.PUBLIC.TRADETYPE_STG;
TRUNCATE TABLE TPCDI_STG.PUBLIC.STATUSTYPE_STG;
TRUNCATE TABLE TPCDI_STG.PUBLIC.TAXRATE_STG;
TRUNCATE TABLE TPCDI_STG.PUBLIC.INDUSTRY_STG;
TRUNCATE TABLE TPCDI_STG.PUBLIC.CUSTOMER_MGMT_STG;
TRUNCATE TABLE TPCDI_STG.PUBLIC.HR_STG;
TRUNCATE TABLE TPCDI_STG.PUBLIC.ACCOUNT_STG;
TRUNCATE TABLE TPCDI_STG.PUBLIC.CUSTOMER_STG;
TRUNCATE TABLE TPCDI_STG.PUBLIC.PROSPECT_STG;

-- TRUNCATE ODS TABLES
TRUNCATE TABLE TPCDI_ODS.PUBLIC.FINWIRE_CMP_ODS;
TRUNCATE TABLE TPCDI_ODS.PUBLIC.FINWIRE_FIN_ODS;
TRUNCATE TABLE TPCDI_ODS.PUBLIC.FINWIRE_SEC_ODS;
TRUNCATE TABLE TPCDI_ODS.PUBLIC.ACCOUNT_ODS;
TRUNCATE TABLE TPCDI_ODS.PUBLIC.CUSTOMER_ODS;

-- TRUNCATE DW TABLES
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_DATE;
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_TIME;
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_TRADE_TYPE;
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_STATUS_TYPE;
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_TAX_RATE;
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_INDUSTRY;
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_BROKER;
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_COMPANY;
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_FINANCIAL;
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_SECURITY;
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_ACCOUNT;
TRUNCATE TABLE TPCDI_WH.PUBLIC.DIM_CUSTOMER;
TRUNCATE TABLE TPCDI_WH.PUBLIC.FACT_PROSPECT;

-- RESET SEQUENCES
CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.DIM_COMPANY_SEQ START WITH = 1 INCREMENT = 1 COMMENT = 'DATABASE SEQUENCE TO SOURCE THE SURROGATE KEY FOR COMPANY.';
CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.DIM_BROKER_SEQ START WITH = 1 INCREMENT = 1 COMMENT = 'DATABASE SEQUENCE TO SOURCE THE SURROGATE KEY FOR BROKER.';
CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.DIM_SECURITY_SEQ START WITH = 1 INCREMENT = 1 COMMENT = 'DATABASE SEQUENCE TO SOURCE THE SURROGATE KEY FOR SECURITY.';
CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.DIM_ACCOUNT_SEQ START WITH = 1 INCREMENT = 1 COMMENT = 'DATABASE SEQUENCE TO SOURCE THE SURROGATE KEY FOR ACCOUNT.';
CREATE OR REPLACE SEQUENCE TPCDI_WH.PUBLIC.DIM_CUSTOMER_SEQ START WITH = 1 INCREMENT = 1 COMMENT = 'DATABASE SEQUENCE TO SOURCE THE SURROGATE KEY FOR CUSTOMER.';

-- DROP STREAMS
-- DROP STAGE STREAMS
DROP STREAM TPCDI_STG.PUBLIC.DATE_STG_STM;
DROP STREAM TPCDI_STG.PUBLIC.TIME_STG_STM;
DROP STREAM TPCDI_STG.PUBLIC.TRADETYPE_STG_STM;
DROP STREAM TPCDI_STG.PUBLIC.STATUSTYPE_STG_STM;
DROP STREAM TPCDI_STG.PUBLIC.TAXRATE_STG_STM;
DROP STREAM TPCDI_STG.PUBLIC.INDUSTRY_STG_STM;
DROP STREAM TPCDI_STG.PUBLIC.CUSTOMER_MGMT_STG_STM;
DROP STREAM TPCDI_STG.PUBLIC.HR_STG_STM;
DROP STREAM TPCDI_STG.PUBLIC.ACCOUNT_STG_STM;
DROP STREAM TPCDI_STG.PUBLIC.CUSTOMER_STG_STM;
DROP STREAM TPCDI_STG.PUBLIC.PROSPECT_STG_STM;

-- DROP ODS STREAMS
DROP STREAM TPCDI_ODS.PUBLIC.ACCOUNT_ODS_STM;
DROP STREAM TPCDI_ODS.PUBLIC.CUSTOMER_ODS_STM;
DROP STREAM TPCDI_ODS.PUBLIC.FINWIRE_CMP_ODS_STM;
DROP STREAM TPCDI_ODS.PUBLIC.FINWIRE_FIN_ODS_STM;
DROP STREAM TPCDI_ODS.PUBLIC.FINWIRE_SEC_ODS_STM;


-- RECREATE STREAMS
-- RECREATE STAGE STREAMS
CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.DATE_STG_STM ON TABLE TPCDI_STG.PUBLIC.DATE_STG;
CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TIME_STG_STM ON TABLE TPCDI_STG.PUBLIC.TIME_STG;
CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TRADETYPE_STG_STM ON TABLE TPCDI_STG.PUBLIC.TRADETYPE_STG;
CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.STATUSTYPE_STG_STM ON TABLE TPCDI_STG.PUBLIC.STATUSTYPE_STG;
CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.TAXRATE_STG_STM ON TABLE TPCDI_STG.PUBLIC.TAXRATE_STG;
CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.INDUSTRY_STG_STM ON TABLE TPCDI_STG.PUBLIC.INDUSTRY_STG;
CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.CUSTOMER_MGMT_STG_STM ON TABLE TPCDI_STG.PUBLIC.CUSTOMER_MGMT_STG;
CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.HR_STG_STM ON TABLE TPCDI_STG.PUBLIC.HR_STG;
CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.ACCOUNT_STG_STM ON TABLE TPCDI_STG.PUBLIC.ACCOUNT_STG;
CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.CUSTOMER_STG_STM ON TABLE TPCDI_STG.PUBLIC.CUSTOMER_STG;
CREATE OR REPLACE STREAM TPCDI_STG.PUBLIC.PROSPECT_STG_STM ON TABLE TPCDI_STG.PUBLIC.PROSPECT_STG;

-- RECREATE ODS STREAMS
CREATE OR REPLACE STREAM TPCDI_ODS.PUBLIC.ACCOUNT_ODS_STM ON TABLE TPCDI_ODS.PUBLIC.ACCOUNT_ODS;
CREATE OR REPLACE STREAM TPCDI_ODS.PUBLIC.CUSTOMER_ODS_STM ON TABLE TPCDI_ODS.PUBLIC.CUSTOMER_ODS;
CREATE OR REPLACE STREAM TPCDI_ODS.PUBLIC.FINWIRE_CMP_ODS_STM ON TABLE TPCDI_ODS.PUBLIC.FINWIRE_CMP_ODS;
CREATE OR REPLACE STREAM TPCDI_ODS.PUBLIC.FINWIRE_FIN_ODS_STM ON TABLE TPCDI_ODS.PUBLIC.FINWIRE_FIN_ODS;
CREATE OR REPLACE STREAM TPCDI_ODS.PUBLIC.FINWIRE_SEC_ODS_STM ON TABLE TPCDI_ODS.PUBLIC.FINWIRE_SEC_ODS;



-- RUN DIM_ACCOUNT, DIM_CUSTOMER HISTORICAL LOAD
CALL TPCDI_WH.PUBLIC.DIM_CUSTOMER_HISTORICAL_SP();
CALL TPCDI_WH.PUBLIC.DIM_ACCOUNT_HISTORICAL_SP();

-- START ALL TASKS
CALL TPCDI_WH.PUBLIC.START_TASKS_SP();
-- EXECUTE TASK TPCDI_WH.PUBLIC.DIM_CUSTOMER_INCREMENTAL_TSK;
-- EXECUTE TASK TPCDI_WH.PUBLIC.DIM_ACCOUNT_TSK;
-- EXECUTE TASK TPCDI_WH.PUBLIC.FACT_PROSPECT_TSK;

-- STOP ALL TASKS
--CALL TPCDI_WH.PUBLIC.STOP_TASKS_SP();
--EXECUTE TASK TPCDI_WH.PUBLIC.DIM_CUSTOMER_TSK SUSPEND;
--EXECUTE TASK TPCDI_WH.PUBLIC.DIM_ACCOUNT_TSK SUSPEND;
--EXECUTE TASK TPCDI_WH.PUBLIC.FACT_PROSPECT_TSK SUSPEND;

-- LOAD FILES
--CALL TPCDI_STG.PUBLIC.DEMO_LOAD_SP();

