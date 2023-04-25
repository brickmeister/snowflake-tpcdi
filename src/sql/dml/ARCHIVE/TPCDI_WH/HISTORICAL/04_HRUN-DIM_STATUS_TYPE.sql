-- DIM_STATUS_TYPE ONLY REQUIRES AN HISTORICAL LOAD

MERGE INTO TPCDI_WH.PUBLIC.DIM_STATUS_TYPE USING
(
SELECT
ST_ID
,ST_NAME
,METADATA$ACTION
,METADATA$ISUPDATE
FROM TPCDI_STG.PUBLIC.STATUSTYPE_STG_STM
) STATUS_TYPE_STG ON TPCDI_WH.PUBLIC.DIM_STATUS_TYPE.ST_ID = STATUS_TYPE_STG.ST_ID
WHEN MATCHED AND
STATUS_TYPE_STG.METADATA$ACTION = 'INSERT' AND STATUS_TYPE_STG.METADATA$ISUPDATE = 'TRUE'
THEN UPDATE SET
DIM_STATUS_TYPE.ST_NAME = STATUS_TYPE_STG.ST_NAME
WHEN NOT MATCHED AND
STATUS_TYPE_STG.METADATA$ACTION = 'INSERT' AND STATUS_TYPE_STG.METADATA$ISUPDATE = 'FALSE'
THEN INSERT
(
  ST_ID
  ,ST_NAME
)
VALUES
(
  STATUS_TYPE_STG.ST_ID
  ,STATUS_TYPE_STG.ST_NAME
)
;