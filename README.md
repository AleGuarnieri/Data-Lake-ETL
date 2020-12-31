# Data Lake Project

## Installation and execution
In order to make use of this project, it is necessary to have an AWS account with related access keys and IAM roles. 

First create an empty bucket on S3 which will be used as sink, then create an EMR cluster and attach a notebook on it to have a processing unit,
then execute the script etl.py on the notebook to perform ETL process.
Note that the execution of the script can cost some money as it involves data extraction and manipulation.

The script can be tested also locally using a subset of the full dataset and having Spark installed locally (instruction for this are not included
in this readme).

## Motivation
The goal of this project is to perform ETL process between data storage (S3 service in AWS) and spark cluster (EMR service in AWS).
Data are extracted from S3, tranformed using an EMR cluster and then loaded back into S3. In this case the ETL could be considered to some extent 
also as ELT, as the EMR cluster can be considered a data lake in which the data are loaded as they are and transformed using "schema-on-read" with Spark. 
The final purpose is to tranform the two source tables into five table which use the star schema to ease possible analytical processes on 
the transformed data.

## Details
The output tables created include one fact table and four dimensional tables.
The source datasets are retrieved from public S3 bucket (log_data and song_data datasets) in JSON format.

## File Description 
etl.py: contains the functions necessary to extract, tranform and load the data

dl.cfg: contains necessary credentials to access AWS services

## Acknowledgements
Udacity provided the course material necessary to implement the project