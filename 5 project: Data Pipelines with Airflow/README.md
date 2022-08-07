# Sparkify's Event Logs Data Pipeline

## Introduction

## Project Summary

A fictional music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines. They have come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in Amazon S3 buckets and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets may consist of CSV or JSON logs that record user activity in the application and store metadata about the songs that have been played.

For this project, I have created a data pipeline using the Airflow python API. The pipeline is dynamic, built from reusable tasks, can be monitored, allows easy backfills, and conducts automated data quality checks.

For illustration purposes you can check out the graph that represents this pipeline's flow:

![Directed Acyclic Graph of this Data Pipeline](https://raw.githubusercontent.com/gabfr/data-engineering-nanodegree/master/4-data-pipelines-with-airflow/L4_project/images/dag.png)

Briefly talking about this ELT process: 
 - Stages the raw data;
 - then transform the raw data to the songplays fact table;
 - and transform the raw data into the dimensions table too;
 - finally, check if the fact/dimensions table has at least one row.

## Project Structure

```
Data Pipeline with Apache Airflow
|
|____dags
| |____ create_tables_dag.py   # DAG for creating tables on Redshift
| |____ create_tables.sql      # SQL CREATE queries
| |____ udac_example_dag.py    # Main DAG for this ETL data pipeline
|
|____plugins
| |____ __init__.py
| |
| |____operators
| | |____ __init__.py          # Define operators and helpers
| | |____ stage_redshift.py    # COPY data from S3 to Redshift
| | |____ load_fact.py         # Execute INSERT query into fact table
| | |____ load_dimension.py    # Execute INSERT queries into dimension tables
| | |____ data_quality.py      # Data quality check after pipeline execution
| |
| |____helpers
| | |____ __init__.py
| | |____ sql_queries.py       # SQL queries for building up dimensional tables
```

## Data sources

We will read basically two main data sources on Amazon S3:

 - `s3://udacity-dend/song_data/` - JSON files containing meta information about song/artists data
 - `s3://udacity-dend/log_data/` - JSON files containing log events from the Sparkify app
 
# The database schema design and ETL pipeline.

In order to enable Sparkify to analyze their data, a Relational Database Schema was created, which can be filled with an ETL pipeline.

The so-called star scheme enables the company to view the user behaviour over several dimensions.
The fact table is used to store all user song activities that contain the category "NextSong". Using this table, the company can relate and analyze the dimensions users, songs, artists and time.

* **Fact Table**: songplays
* **Dimension Tables**: users, songs, artists and time.

 
