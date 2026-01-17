# Serverless TLQ Data Processing Pipeline (AWS Lambda)

This project implements a **serverless Transform–Load–Query (TLQ) data processing pipeline** using **AWS Lambda**, **Amazon S3**, and **Amazon RDS (MySQL)**.  
The pipeline ingests large-scale Spotify CSV data, performs schema normalization and feature engineering, loads the transformed data into a relational database, and exposes analytical queries via a serverless query service.

The system was designed to support **performance and cost analysis of cloud application architectures**, including serverless execution, batching strategies, and database-backed analytics.

---

## Dataset

This project uses the **900K Spotify Tracks dataset** from Kaggle:

**Dataset:** 900k Spotify Tracks  
**Source:** https://www.kaggle.com/datasets/devdope/900k-spotify  

The dataset contains hundreds of thousands of Spotify tracks with metadata such as track name, artists, popularity, danceability, energy, explicit content flags, and duration information.

---

## Architecture Overview

```
Raw CSV (S3)
   │
   ▼
Transform Lambda
   │   (schema normalization, feature engineering)
   ▼
Transformed CSV (S3)
   │
   ▼
Load Lambda
   │   (batched inserts)
   ▼
MySQL Database (Amazon RDS)
   │
   ▼
Query Lambda
       (aggregations & filters)
```

Each stage is implemented as an **independent AWS Lambda function**, enabling modular deployment and performance evaluation.

---

## Components

### Transform Stage (`transform.py`)
- Streams raw CSV data from Amazon S3
- Normalizes heterogeneous column names across datasets
- Performs feature engineering:
  - Duration parsing (mm:ss, hh:mm:ss, milliseconds)
  - Popularity tier classification
  - Danceability and energy labeling
  - Explicit vs. clean content labeling
- Writes transformed CSV output to S3 using `/tmp` storage
- Designed for **low memory overhead** and **large datasets**

### Load Stage (`load.py`)
- Reads transformed CSV files from Amazon S3
- Creates relational schema if it does not exist
- Loads data into **MySQL (Amazon RDS)** using batched inserts
- Uses configurable batch sizes for throughput optimization
- Ensures transactional integrity with explicit commits

### Query Stage (`query.py`)
- Executes analytical SQL queries over the loaded dataset
- Supports:
  - Top artists by popularity
  - Aggregated metrics by category
  - Threshold-based filtering
  - Explicit vs. clean track comparisons
- Returns results as structured JSON
- Compatible with **CLI**, **API Gateway**, and **SAAF benchmarking**

---

## Technologies Used

- AWS Lambda
- Amazon S3
- Amazon RDS (MySQL)
- Python 3
- Boto3
- PyMySQL
- SQL (aggregation, filtering, grouping)
- CSV streaming and transformation
- Linux-based execution environment

---

## Environment Variables

Each Lambda function relies on environment configuration.

### Shared
RAW_BUCKET  
TRANSFORMED_BUCKET  

### Load & Query
DB_HOST  
DB_USER  
DB_PASSWORD  
DB_NAME  

### Load (Optional)
BATCH_SIZE (default: 1000)

---

## How to Use

### 1. Upload Raw CSV Data

Upload a Spotify CSV file to the configured raw S3 bucket:

```
s3://<RAW_BUCKET>/spotify_tracks.csv
```

---

### 2. Run Transform Lambda

Invoke the Transform function with:

```json
{
  "bucket": "<RAW_BUCKET>",
  "key": "spotify_tracks.csv"
}
```

Output:
```
s3://<TRANSFORMED_BUCKET>/spotify_tracks_transformed.csv
```

---

### 3. Run Load Lambda

Invoke the Load function with:

```json
{
  "bucket": "<TRANSFORMED_BUCKET>",
  "key": "spotify_tracks_transformed.csv"
}
```

Result:
- Relational table `tracks` is created if needed
- All transformed rows are inserted into MySQL

---

### 4. Run Queries

Example query payloads:

Top artists by popularity:
```json
{
  "action": "top_artists",
  "metric": "popularity",
  "limit": 10
}
```

Filtered songs:
```json
{
  "action": "songs_filtered",
  "min_popularity": 70,
  "min_danceability": 60,
  "limit": 25
}
```

Responses are returned as structured JSON arrays.

---

## Design Highlights

- Streaming transformations without loading full datasets into memory
- Serverless batching strategies for efficient database ingestion
- Relational analytics executed entirely via AWS Lambda
- Extensible schema for additional metrics and features
- Designed for scalability, performance benchmarking, and cost analysis

---

## Results

Work in progress. Stay tuned!
