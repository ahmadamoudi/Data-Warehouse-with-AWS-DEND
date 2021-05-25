# DataWarehouse with AWS DEND

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that 

1. extracts their data from S3, 
2. stages them in Redshift, 
3. transforms data into a set of dimensional tables for business analytics. 

You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# Dataset

You'll be working with two datasets that reside in **S3 with the following link:

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`
- Log data json path: `s3://udacity-dend/log_json_path.json` to load the Log data.

## Song Data

The first dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```txt
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like:

```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

## Log Data

The log data are in JSON format files generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. The log files are partitioned by year and month. For example, here are filepaths to two files in this dataset:

```txt
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like:

![img](img/log-data.png)

# Project Development

The project is developed in a **local enviornment (MacOS)** and based on pure **Python SDK** codes. 

The steps, including creating a cluster, create and load tables, testing, were pipelined in a **Jupyter notebook** [IaC.ipynb](https://github.com/ahmadamoudi/Data-Warehouse-with-AWS-DEND/blob/master/IaC.ipynb)

## Overview of the steps

1. Define queries in `sql_queries.py` to **create empty tables, copy data from S3 and insert data into tables**
2. Set up AWS environment by:
   1. **Create IAM user** and take note of **key and secret** (this is the only step to be done on AWS web interface)
   2. Copy key and secret to `dwh.cfg`
   3. **Create IAM role** and **Red shift cluster** with pre-defined DWH parameters in `dwh.cfg`
   4. Copy the `DWH_ENTPONT` and  `IAM_ROLE` displayed to `dwh.cfg`
   5. Connect the cluster with command line `sql tool`
3. Run `create_table.py` to **create empty tables**
4. Run `etl.py` (no modification is needed) to **1) copy data from S3 to staging 2) insert data from staging to tables**
5. **Test** the created tables with `sql` command
6. **Clean up** resources by deleting the cluster and IAM role

## Files

* `IaC.ipynb` (Infrastructure as a Code)
* `sql_queries.py`
* `create_tables.py`  (*no modification is needed*)
* `etl.py` (*no modification is needed*)
* `dwh.cfg` it contains your key and secret
