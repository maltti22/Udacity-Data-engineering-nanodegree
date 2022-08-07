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

## Data sources

We will read basically two main data sources on Amazon S3:

 - `s3://udacity-dend/song_data/` - JSON files containing meta information about song/artists data
 - `s3://udacity-dend/log_data/` - JSON files containing log events from the Sparkify app

## How to Run
1. Create a Redshift cluster on your AWS account
2. Turn on Airflow by running Airflow/start.sh
3. Create AWS and Redshift connections on Airflow Web UI
4. Run create_table _dag DAG to create tables on Redshift
5. Run udac_example _dag DAG to trigger ETL data pipeline

 
# The database schema design and ETL pipeline.

In order to enable Sparkify to analyze their data, a Relational Database Schema was created, which can be filled with an ETL pipeline.

The so-called star scheme enables the company to view the user behaviour over several dimensions.
The fact table is used to store all user song activities that contain the category "NextSong". Using this table, the company can relate and analyze the dimensions users, songs, artists and time.

* **Fact Table**: songplays
* **Dimension Tables**: users, songs, artists and time.

 
