import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp,
    cast, when, coalesce, udf
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, LongType, FloatType
import glob

def get_latest_json(pattern: str) -> str:
    """Get the most recent file matching the pattern"""
    files = glob.glob(pattern)
    if not files:
        print(f"No files found matching {pattern}")
        return None
    # Return the latest file
    return max(files, key=os.path.getctime)

def process_tmdb(spark, repo_root: str):
    """Process TMDB trending data"""
    print("\n=== Processing TMDB Data ===")
    
    tmdb_pattern = os.path.join(repo_root, "data/raw/cinema/tmdb/trending_*.json")
    tmdb_file = get_latest_json(tmdb_pattern)
    
    if not tmdb_file:
        print("No TMDB files found. Skipping TMDB processing.")
        return
    
    print(f"Reading TMDB file: {tmdb_file}")
    
    # Read raw TMDB JSON (single JSON object with results array)
    raw_df = spark.read.option("multiline", "true").json(tmdb_file)
    raw_df.printSchema()
    
    # Extract results array
    df = raw_df.select(explode(col("results")).alias("movie")).select("movie.*")
    
    # Select and rename columns
    tmdb_df = df.select(
        col("id").alias("tmdb_id"),
        col("title"),
        col("overview"),
        col("release_date"),
        col("vote_average"),
        col("vote_count"),
        col("popularity"),
        col("genre_ids"),
        col("original_language"),
        col("media_type")
    )
    
    # Normalize release_date to UTC timestamp (handle nulls and invalid dates)
    tmdb_df = tmdb_df.withColumn(
        "release_date",
        when(col("release_date").isNotNull(),
             to_timestamp(col("release_date"), "yyyy-MM-dd"))
        .otherwise(None)
    )
    
    print(f"\nTMDB schema after processing:")
    tmdb_df.printSchema()
    
    row_count = tmdb_df.count()
    print(f"TMDB row count: {row_count}")
    
    # Save as Parquet
    tmdb_out = os.path.join(repo_root, "data/formatted/cinema/tmdb")
    print(f"Saving TMDB Parquet to: {tmdb_out}")
    tmdb_df.write.mode("overwrite").parquet(tmdb_out)
    print("TMDB Parquet saved successfully.")
    
    return tmdb_df


def clean_imdb_rating(val):
    """Clean IMDB rating string to float"""
    if val is None or val == "N/A":
        return None
    try:
        return float(val)
    except:
        return None


def clean_imdb_votes(val):
    """Clean IMDB votes string to integer"""
    if val is None or val == "N/A":
        return None
    try:
        return int(val.replace(",", ""))
    except:
        return None


def clean_runtime(val):
    """Clean runtime string to integer (remove ' min')"""
    if val is None or val == "N/A":
        return None
    try:
        return int(val.split()[0])
    except:
        return None


def clean_boxoffice(val):
    """Clean BoxOffice string to integer (remove $ and commas)"""
    if val is None or val == "N/A":
        return None
    try:
        # Remove $ and commas
        cleaned = val.replace("$", "").replace(",", "")
        return int(cleaned)
    except:
        return None


def process_omdb(spark, repo_root: str):
    """Process OMDB details data"""
    print("\n=== Processing OMDB Data ===")
    
    omdb_pattern = os.path.join(repo_root, "data/raw/cinema/omdb/details_*.json")
    omdb_file = get_latest_json(omdb_pattern)
    
    if not omdb_file:
        print("No OMDB files found. Skipping OMDB processing.")
        return
    
    print(f"Reading OMDB file: {omdb_file}")
    
    # Read raw OMDB JSON (it's an array of objects)
    raw_df = spark.read.option("multiline", "true").json(omdb_file)
    raw_df.printSchema()
    
    # Flatten nested "omdb" object - get individual columns from omdb struct
    omdb_df = raw_df.select(
        col("tmdb_id"),
        col("imdb_id"),
        col("omdb.Title"),
        col("omdb.Year"),
        col("omdb.Rated"),
        col("omdb.Released"),
        col("omdb.Runtime"),
        col("omdb.Genre"),
        col("omdb.Director"),
        col("omdb.Actors"),
        col("omdb.Plot"),
        col("omdb.Language"),
        col("omdb.Country"),
        col("omdb.imdbRating"),
        col("omdb.imdbVotes"),
        col("omdb.BoxOffice"),
        col("omdb.Metascore")
    )
    
    # Register UDFs for cleaning
    clean_rating_udf = udf(clean_imdb_rating, FloatType())
    clean_votes_udf = udf(clean_imdb_votes, IntegerType())
    clean_runtime_udf = udf(clean_runtime, IntegerType())
    clean_boxoffice_udf = udf(clean_boxoffice, IntegerType())
    
    # Apply type conversions and cleaning
    omdb_df = omdb_df \
        .withColumn("imdbRating", clean_rating_udf(col("imdbRating"))) \
        .withColumn("imdbVotes", clean_votes_udf(col("imdbVotes"))) \
        .withColumn("Runtime", clean_runtime_udf(col("Runtime"))) \
        .withColumn("BoxOffice", clean_boxoffice_udf(col("BoxOffice"))) \
        .withColumn("Year", when(col("Year") != "N/A", cast(col("Year"), "int")).otherwise(None))
    
    # Convert Released date string to UTC timestamp
    omdb_df = omdb_df.withColumn(
        "Released",
        when(col("Released").isNotNull() & (col("Released") != "N/A"),
             to_timestamp(col("Released"), "dd MMM yyyy"))
        .otherwise(None)
    )
    
    print(f"\nOMDB schema after processing:")
    omdb_df.printSchema()
    
    row_count = omdb_df.count()
    print(f"OMDB row count: {row_count}")
    
    # Show sample data to verify conversions
    print("\nOMDB sample data:")
    omdb_df.select("tmdb_id", "imdb_id", "Title", "imdbRating", "imdbVotes", "Runtime", "BoxOffice").show(3, truncate=False)
    
    # Save as Parquet
    omdb_out = os.path.join(repo_root, "data/formatted/cinema/omdb")
    print(f"Saving OMDB Parquet to: {omdb_out}")
    omdb_df.write.mode("overwrite").parquet(omdb_out)
    print("OMDB Parquet saved successfully.")
    
    return omdb_df


def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("CinemaFormatting") \
        .getOrCreate()
    
    print("SparkSession initialized: CinemaFormatting")
    
    # Determine repo root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, ".."))
    print(f"Repository root: {repo_root}")
    
    # Ensure output directories exist
    os.makedirs(os.path.join(repo_root, "data/formatted/cinema/tmdb"), exist_ok=True)
    os.makedirs(os.path.join(repo_root, "data/formatted/cinema/omdb"), exist_ok=True)
    
    try:
        # Process both datasets
        process_tmdb(spark, repo_root)
        process_omdb(spark, repo_root)
        
        print("\n=== Formatting Complete ===")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
