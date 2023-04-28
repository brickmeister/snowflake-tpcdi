CREATE OR REPLACE PROCEDURE TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_I_SP(scale float,batches float,wait float)
	returns float
	language javascript
	as
	$$
	var tpcdi_scale = SCALE
	// Load incremental files	
	var batch_counter = 2;
	while (batch_counter <= BATCHES)	
	{
		var incrm_stmt = snowflake.createStatement(
			{sqlText: "COPY INTO TPCDI_STG.PUBLIC.WATCH_HISTORY_STG FROM @TPCDI_FILES//tpcdi/sf=" + tpcdi_scale + "/Batch" + batch_counter + "/WatchHistory FILE_FORMAT = (FORMAT_NAME = 'TXT_PIPE') ON_ERROR = SKIP_FILE"}
			);
		incrm_stmt.execute();
		// insert wait here
		stmt = snowflake.createStatement({sqlText:"call system$wait(" + WAIT + ", 'SECONDS')"});
		rs = stmt.execute();	
		batch_counter++
	}
	// Suspend Load Task
	var task_stmt = snowflake.createStatement({sqlText: "ALTER TASK TPCDI_STG.PUBLIC.LOAD_WATCH_HISTORY_I_" + tpcdi_scale + "_TSK SUSPEND"});
	task_stmt.execute();
	return "All incremental watch_history files have been loaded.";
	$$
;
