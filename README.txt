# Football Analysis using AWS S3, Redshift

## Table of Contents

- [Description](#description)
- [Code-Flow](#code-flow)
- [Module-Explanation](#module-explanation)
- [Future-Development](#future-development)

## Description

This project demonstrates Sports analytics for football clubs and analysts can offer tactical insights, optimize player performance analysis, and improve decision-making 
using match and player data.

## Code-Flow

The project is structured in a way that AWS Commands and Python scripts works together to Extract, Transform, Load the input data.

* Use the StatsBomb Open Data to study player and team performances. 
* Build a data pipeline to ingest player and match data, clean it for inconsistencies, and transform it for analysis. 
* Load raw data into Google Cloud Storage(use s3 bucket), preprocess it, and store results in BigQuery(Redshift). 
* Create dashboards in Looker(Quicksight) to visualize team strategies, player efficiency, and game outcomes. 
* Focus on metrics like ball possession, player heatmaps, passes, and shots. 
* Create dashboards to visualize game highlights, team strengths, and individual player contributions.(Quicksights)

## Module-Explanation

- `main.py`: The main script that contains functions to CRUD Operations for AWS S3 Buckets and Objests.
- `setup.py`: The script contains the required modules/libraries to be installed for the project.
- `dist/etl.py`: This script is used to perform ETL and load the data into AWS Redshift.
- `data/`: This directory contains the dataset used for the ETL process.

## Future-Development

1. Automation Codeflow - To automate the codeflow process to continuously check for input data and process and perform ETL operations.
2. Large Datasets - To make use of large and unstructured data to perform ETL Operations.
3. AWS Glue - AWS Glue, Glue Crawler, Glue catalog to crawl data from AWS S3 and load into AWS Redshift.
4. Dashboard - To integrate AWS Redshift with AWS Quicksight for Performance analysis, Player performance, heatmap on the field, Player Statistics, Team Performance etc.