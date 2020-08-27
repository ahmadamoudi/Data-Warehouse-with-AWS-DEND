# DataWarehouse with AWS

---

In this project, data is stored in S3 to preparation tables in Redshift and SQL statements will be executed to create the analysis tables from these preparation tables.



## Project Repository files
---

In the repository files you can see this list files

- create_tables.py
- sql_queries.py
- etl.py
- dwh.cfg
- README.md


## ETL Process.
---

The ETL process is the file to run all pipeline to the project, connect to database, loads data from S3 buckets/Redshift and inserts statements from staging tables