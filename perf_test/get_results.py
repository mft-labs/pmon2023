#!/opt/B2B/anaconda3/bin/python
from pandmzdb import con, pd
from datetime import datetime

# fileactivity table has key columns like remotefile, filesize, start and end time, time taken to run, run_id
fileactivity_sql = 'select * from fileactivity'

# run_history table has the summary of the run
run_history_sql = 'select * from run_history'

# Dataframe of the fileactivity table for analysis
df_file_activity = pd.read_sql_query(fileactivity_sql,con)

# Dataframe of the run_history table for analysis
df_summary = pd.read_sql_query(run_history_sql,con)

# Display the count of successful and failed file transfers
df_file_activity.groupby('status').count()

# Get the dcs that the files ran against by getting the 1st 5 characters of the remotefile name
run_dc = df_file_activity.remotefile.str[:5]

# Distribution of files to each dc
run_dc.value_counts()

# This would give us the runid that use have to use below to get summary for this run
runid = int(df_file_activity.run_id.unique())

# Summary of the latest run
summary = df_summary[df_summary.run_id.isin([runid])]
curtime = datetime.now()
# write the output to excel
wb = pd.ExcelWriter('SFTP_TEST_RPT_%s.xlsx' %(curtime.strftime('%Y%m%d_%H%M%S')), engine='xlsxwriter')
df_file_activity.to_excel(wb, sheet_name='Send_activity', index=False)
summary.to_excel(wb, sheet_name='Summary', index=False)
wb.save()

