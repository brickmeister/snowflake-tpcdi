-- DELETE PRIOR ROWS FROM TABLE; TRUNCATE IS NOT USED BECAUSE IT RESETS FILE LOAD HISTORY

DELETE FROM TPCDI_STG.PUBLIC.CUSTOMER_STG;

-- COPY INTO TABLE

COPY INTO TPCDI_STG.PUBLIC.CUSTOMER_STG
FROM @TPCDI_FILES/load/customer/
FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE')
PATTERN='.*02.txt'
; 
