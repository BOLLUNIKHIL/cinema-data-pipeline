# Cinema Data Pipeline

End-to-end Big Data pipeline that fetches trending movie data from TMDB and OMDB APIs, processes it with Apache Spark, orchestrates it with Apache Airflow, and exposes results via an Elasticsearch/Kibana dashboard.

## Tech stack

- Python 3.12
- PySpark 4.1.2
- Apache Airflow 3.2.2
- Elasticsearch 8.13.0
- Kibana 8.13.0
- Docker

## Architecture

- **Ingestion:** fetches trending movies from TMDB and details from OMDB, stores raw JSON in `data/raw/cinema/`
- **Formatting:** Spark job converts raw JSON to Parquet and normalizes types and dates to UTC
- **Combination:** Spark job joins both datasets, computes KPIs and movie recommendations
- **Indexing:** pushes enriched data into the Elasticsearch index `cinema_movies`

## Datalake structure

```
data/raw/cinema/tmdb/
data/raw/cinema/omdb/
data/formatted/cinema/tmdb/
data/formatted/cinema/omdb/
data/usage/cinema/movie_enriched/
```

## How to run

1. Prerequisites
   - Activate your Python virtual environment
   - Start Docker with Elasticsearch and Kibana

2. Run the pipeline
   - Trigger the Airflow DAG `cinema_data_pipeline` to run the full pipeline

3. Or run scripts manually (in order)
   - `python scripts/ingest_tmdb.py`
   - `python scripts/format_spark.py`
   - `python scripts/combine_spark.py`
   - `python scripts/index_elasticsearch.py`

## Dashboard

- Kibana: http://localhost:5601
- Index: `cinema_movies`
