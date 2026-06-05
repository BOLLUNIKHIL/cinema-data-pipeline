import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def convert_to_elastic_doc(row_dict):
    """
    Convert a Spark row dict to an Elasticsearch-compatible dict.
    Handles timestamp/date conversion and array fields.
    """
    doc = {}
    for key, value in row_dict.items():
        if value is None:
            # Skip None values
            continue
        elif key == "genre_ids":
            # Convert genre_ids to list of integers
            if isinstance(value, list):
                doc[key] = [int(g) if isinstance(g, (int, float)) else g for g in value]
            else:
                doc[key] = value
        elif key in ["release_date", "Released"]:
            # Convert timestamp to ISO format string
            if hasattr(value, "isoformat"):
                doc[key] = value.isoformat()
            elif isinstance(value, str):
                doc[key] = value
            else:
                doc[key] = str(value)
        else:
            doc[key] = value
    return doc


def main():
    print("=== Cinema Movies Elasticsearch Indexing ===\n")
    
    # Determine repo root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, ".."))
    print(f"Repository root: {repo_root}")
    
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("CinemaElasticsearch") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # Read enriched Parquet
    enriched_path = os.path.join(repo_root, "data/usage/cinema/movie_enriched")
    print(f"\nReading enriched Parquet from: {enriched_path}")
    
    try:
        df = spark.read.parquet(enriched_path)
        row_count = df.count()
        print(f"Loaded {row_count} rows")
        
        # Convert to list of dicts
        print("\nConverting Spark DataFrame to Python dicts...")
        rows = df.collect()
        docs = [convert_to_elastic_doc(row.asDict()) for row in rows]
        print(f"Converted {len(docs)} documents")
        
        # Connect to Elasticsearch
        print("\nConnecting to Elasticsearch at http://localhost:9200...")
        es = Elasticsearch(["http://localhost:9200"])
        
        # Check cluster health
        try:
            health = es.cluster.health()
            print(f"Cluster status: {health['status']}")
        except Exception as e:
            print(f"Warning: Could not check cluster health: {e}")
        
        # Define index name and mapping
        index_name = "cinema_movies"
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "tmdb_id": {"type": "integer"},
                    "imdb_id": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "overview": {"type": "text"},
                    "release_date": {"type": "date"},
                    "vote_average": {"type": "float"},
                    "vote_count": {"type": "integer"},
                    "popularity": {"type": "float"},
                    "genre_ids": {"type": "integer"},
                    "original_language": {"type": "keyword"},
                    "media_type": {"type": "keyword"},
                    "Director": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "Actors": {"type": "text"},
                    "Genre": {"type": "keyword"},
                    "Runtime": {"type": "integer"},
                    "Rated": {"type": "keyword"},
                    "imdbRating": {"type": "float"},
                    "imdbVotes": {"type": "integer"},
                    "BoxOffice": {"type": "long"},
                    "Metascore": {"type": "keyword"},
                    "Language": {"type": "keyword"},
                    "Country": {"type": "keyword"},
                    "Plot": {"type": "text"},
                    "Year": {"type": "keyword"},
                    "Released": {"type": "date"},
                    "rating_diff": {"type": "float"},
                    "box_office_per_vote": {"type": "float"},
                    "popularity_score": {"type": "float"}
                }
            }
        }
        
        # Delete index if exists (clean re-runs)
        print(f"\nPreparing index '{index_name}'...")
        if es.indices.exists(index=index_name):
            print(f"Index exists. Deleting...")
            es.indices.delete(index=index_name)
            print(f"Index deleted.")
        
        # Create index with mapping
        print(f"Creating index with explicit mapping...")
        es.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created successfully.")
        
        # Prepare bulk operations
        print(f"\nBulking {len(docs)} documents to Elasticsearch...")
        actions = [
            {
                "_index": index_name,
                "_id": str(doc.get("tmdb_id")),
                "_source": doc
            }
            for doc in docs
        ]
        
        # Bulk index
        success_count, error_list = bulk(es, actions, raise_on_error=False)
        print(f"Bulk indexing completed: {success_count} succeeded")
        
        # Refresh index to make documents searchable
        print("Refreshing index...")
        es.indices.refresh(index=index_name)
        
        if error_list:
            print(f"Errors encountered: {len(error_list)}")
            for error in error_list[:5]:
                print(f"  - {error}")
        
        # Verify index
        print(f"\n=== Verification ===")
        index_stats = es.indices.stats(index=index_name)
        indexed_count = index_stats["indices"][index_name]["primaries"]["docs"]["count"]
        print(f"Documents in index '{index_name}': {indexed_count}")
        
        # Show mapping
        print(f"\n=== Index Mapping ===")
        current_mapping = es.indices.get_mapping(index=index_name)
        print(f"Mapping applied successfully to '{index_name}'")
        
        # Sample query
        print(f"\n=== Sample Query ===")
        print(f"Querying top movies by popularity_score...")
        query_result = es.search(
            index=index_name,
            body={
                "size": 5,
                "sort": [{"popularity_score": {"order": "desc"}}],
                "_source": ["tmdb_id", "title", "popularity_score", "vote_average", "imdbRating"]
            }
        )
        
        print(f"Top 5 movies by popularity_score:")
        for i, hit in enumerate(query_result["hits"]["hits"], 1):
            source = hit["_source"]
            print(f"  {i}. {source.get('title')} - popularity_score: {source.get('popularity_score')}")
        
        print(f"\n=== Indexing Complete ===")
        spark.stop()
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        spark.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
