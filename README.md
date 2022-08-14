
## Data orchestration from importing raw data into GCS Data Lake followed by Data Ingestion to BigQuery using Apache Airflow


### Setting up Airflow locally

* Setting up Airflow with Docker-Compose
* More information in the [airflow folder](airflow)


### Ingesting data to GCP with Airflow

* Extraction: Download and unpack the data
* Pre-processing: Convert this raw data to parquet
* Upload the parquet files to GCS
* Create an external table in BigQuery

### Ingesting data to Local Postgres with Airflow

* Converting the ingestion script for loading data to Postgres to Airflow DAG


