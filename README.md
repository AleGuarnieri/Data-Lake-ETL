# Data Lake Project

## Motivation
This goal of this project is to perform ETL process between data storage (S3 service in AWS) and spark cluster (EMR service in AWS).
Data are extracted from S3, tranformed in EMR and then loaded back into S3.
The purpose is to tranform the two source tables into five table which use the star schema to ease possible analytical processes on 
the transformed data.

## Details
The output tables created include one fact table and four dimensional tables.
The source datasets are retrieved from public s3 bucket (log_data and song_data datasets) in JSON format.

## File Description 
etl.py: contains the functions necessary to extract, tranform and load the data. 


## Acknowledgements
Udacity provided the course material necessary to implement the project