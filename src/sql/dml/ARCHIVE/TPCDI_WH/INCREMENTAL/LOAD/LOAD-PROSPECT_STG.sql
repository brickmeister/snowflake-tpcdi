-- DELETE PRIOR ROWS FROM TABLE; TRUNCATE IS NOT USED BECAUSE IT RESETS FILE LOAD HISTORY

DELETE FROM TPCDI_STG.PUBLIC.PROSPECT_STG;

-- COPY INTO TABLE

COPY INTO TPCDI_STG.PUBLIC.PROSPECT_STG
FROM @TPCDI_FILES/load/prospect/
FILE_FORMAT = (FORMAT_NAME = 'TXT_CSV')
PATTERN='.*02.csv'
;