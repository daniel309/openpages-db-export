# IBM OpenPages Query to Database Table Export Tool

A Python program that, given an OpenPages pseudo SQL query, creates a table in Db2 based on 
the schema of the result and loads the query result into this table. 

Designed to run periodically as job (CP4D scheduled job, cron, ...). Loads the query result into a 
shadow table so that the previous import iteration is still available for querying 
while the update job is running. Also uses Db2 bulk insert API for best throughput. 

Parameters are environment variables (TABLE, QUERY), the configuration is in the .py itself. 
The idea is to create one job instance per query so that the export can run in parallel 
per table/query combination if needed. 

Throughput I measured is about 10k rows per 30s. There should be no limit on result size, 
everything is streamed and batched, no intermediate result materialisation happens. 


# Useful Links and Background Information

OpenPages API Overview: https://www.ibm.com/docs/en/opw/8.2.0?topic=guide-openpages-api-documentation

This program uses the OpenPages GRC REST API, specifically the '/query' path. The REST API 
is documented under https://www.ibm.com/docs/en/opw/8.2.0?topic=apis-openpages-rest-api, 
a PDF version can be downloaded from https://public.dhe.ibm.com/software/data/cognos/documentation/openpages/en/8.2.0/GRC_REST_API.pdf

The OpenPages Query Syntax is documented in the OpenPages JavaDoc: 
https://public.dhe.ibm.com/software/data/cognos/documentation/openpages/en/8.2.0.4/javadoc/com/ibm/openpages/api/query/package-summary.html


# Scheduling This Program to Run Regularly

There are plenty of job schedulers available on Linux, MacOS and Windows. Please
refer to their documentation. For Linux, a popular choice is cron: 
https://opensource.com/article/17/11/how-use-cron-linux


# Example How to Schedule this Program as a Job in Cloud Pak for Data

![Job Runs](res/job_runs.png?raw=true "Job Runs")

![Job Settings](res/job_settings.png?raw=true "Job Settings")



