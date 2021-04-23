# gcpwtrace

This code is using the wtrace tool to do RUM on the client performance. all the data is ingest into BQ for future analysis.


Step 1: Get Wtrace Access

Step 2: Using the collectagentinfosavefile.py to save the agent information as a .CSV file, then load the file to BG Table as a Dimension Table, which will refer by following setp.

Step 3: Using the wtracebq.py to start the tracing. before the job, please prepare the service account and place the sa credentials file in the same folder path as the wtracebq.py file. Base on your environment, edit the BQ database & table name.
