# Transformation Pipeline To Create Iceberg DataLake from Google Kaggle Dataset 

**THIS IS STILL VERY MUCH UNDER DEVELOPMENT. SO MUCH DOCUMENTATION TO DO**

[Google Merchandise Store Dataset](https://www.kaggle.com/c/ga-customer-revenue-prediction)

*This project provides a sandbox with all of the components to build a foundational data layer under the iceberg open table format. The goal is to demonstrate that with iceberg, you can stream data into file storage while remaining compliant with the principles of ACID transactions that historically were only supported by databases. With this technology, you can now easily handle much of the heavy lifting of data transformation and modeling before loading the data into your warehouse of choice for advanced analysis, reporting, and activation.*

## Gen Dataset

- Containts scripts to normalize the dataset and create flat sessions and pageviews fact tables 

## Pageviews Loader

- DLT Pipeline to load the pageview files to S3 as Parquet files under the Iceberg Format

## Sessions Loader

- DLT Pipeline to load the pageview files to S3 as Parquet files under the Iceberg Format

## JSON Event Stream

- After creating an Iceberg Data Lake from the Google Analytics Kaggle Dataset, this folder contains scripts to mock streaming event data into these iceberg tables. 

## Frozen Swamp Models

- DBT Project using the athena aws glue iceberg adapter to transform the data to create a user dimensions table. 


## Snowflake Icelake Setup

- Queries to set up a connection between your iceberg datalake and Snowflake so you can run queries against your iceberg tables with Snowflake