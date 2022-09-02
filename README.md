# gcpwtrace

This code is using the wtrace tool to do RUM on the client performance. all the data is ingest into BQ for future analysis.


Step 1: Get Wtrace Access

Step 2: Using the collectagentinfosavefile.py to save the agent information as a .CSV file, then load the file to BG Table as a Dimension Table, which will refer by following setp.

Step 3: Edit config.ini for credentials and BQ tables

Step 4: wtrace.py to start the tracing, or edit and use run.sh

