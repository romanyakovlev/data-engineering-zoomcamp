# Spotify Top Charts Project

## Project Goal

This project is developed to analyze the data from "TOP 200" charts based on [kaggle dataset](https://www.kaggle.com/datasets/dhruvildave/spotify-charts)

The goal is creating 4 charts:
1. Show the most popular song in Global region (the most frequent top 1 song)
2. Show the most popular artist in Global region (the sum of the most frequent top 1 songs)
3. Show the most popular song in all regions (the most frequent top 1 song)
4. Show the most popular artist in all regions (the sum of the most frequent top 1 songs)

## The results

![page 1-2](https://github.com/romanyakovlev/data-engineering-zoomcamp/blob/main/project/imgs/1.jpg?raw=true)
![page 3-4](https://github.com/romanyakovlev/data-engineering-zoomcamp/blob/main/project/imgs/2.jpg?raw=true)

# Project Structure

![flowchart](https://github.com/romanyakovlev/data-engineering-zoomcamp/blob/main/project/imgs/flowchart.png?raw=true)

## What is used

* Google Cloud Platform (GCP):        
  * Google Cloud Storage (GCS): Data Lake - stores raw data
  * BigQuery: Data Warehouse - stores optimized tables
  * DataProc - runs PySpark jobs in cloud cluster
* Terraform: Infrastructure-as-Code (IaC)
* Prefect: Workflow Orchestration
* Spark: Distributed Processing
* Looker studio: Data Visualisation

# How to run

## Prerequisites

To run project you need to:

1. Install the following requirements:
* [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
* [Terraform](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli)

2. Create an account in [Google Cloud Platform (GCP)](https://cloud.google.com/) 
3. Create project in GCP
4. Create service acccount with following roles:
* BigQuery Admin
* Dataproc Administrator
* Dataproc Editor
* Editor
* Storage Admin
* Storage Object Admin 

3. Generate service account credentials in json format.

4. Create .env file in root directory:
```sh
"     
GOOGLE_APPLICATION_CREDENTIALS=

PROJECT_ID=

KAGGLE_USERNAME=
KAGGLE_KEY=

PREFECT_CLOUD_API_KEY=
PREFECT_CLOUD_WORKSPACE=

CLUSTER_NAME=
CLUSTER_REGION=
" > .env

```

5. Specify `GOOGLE_APPLICATION_CREDENTIALS` as variable path to service account credentials.
6. Speficy `PROJECT_ID` as project id in GCP.

## Initialize Infrastructure

1. Run 
