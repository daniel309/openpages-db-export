# IBM OpenPages Query to Database Table Export Tool

Python program that, given an OpenPages pseudo SQL query, creates a table in Db2 based on 
the schema of the result and loads the query result into this table. 

Designed to run periodically as job (CP4D scheduled job, cron, ...). Loads the query result into a 
shadow table so that the previous import iteration is still available for querying 
while the update job is running. 

Parameters are environment variables (TABLE, QUERY), configuration is in the .py itself. 
The idea is to create one job instance per query so that the export can run in parallel 
per table/query combination. 

Throughput I measured is about 10k rows per 30s. There should be no limit on result size 
of the query, everything is streamed and batched, no intermediate result materialisation
happens. 


# Example How to Schedule this Program as a Job in Cloud Pak for Data

![Job Runs](res/job_runs.png?raw=true "Job Runs")

![Job Settings](res/job_settings.png?raw=true "Job Settings")


Note: there are plenty of other schedulers available on Linux, MacOS and Windows. Please
refer to their documentation. For Linux, a popular choice is cron: 
https://opensource.com/article/17/11/how-use-cron-linux
