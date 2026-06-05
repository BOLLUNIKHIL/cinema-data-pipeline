import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, round as spark_round, lit, udf
)
from pyspark.sql.types import StringType

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("CinemaCombination") \
        .getOrCreate()
    
    print("SparkSession initialized: CinemaCombination")
    
    # Determine repo root
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.abspath(os.path.join(script_dir, ".."))
    print(f"Repository root: {repo_root}")
    
    # Ensure output directory exists
    output_dir = os.path.join(repo_root, "data/usage/cinema/movie_enriched")
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Read formatted Parquet files
        tmdb_path = os.path.join(repo_root, "data/formatted/cinema/tmdb")
        omdb_path = os.path.join(repo_root, "data/formatted/cinema/omdb")
        
        print(f"\n=== Reading Formatted Data ===")
        print(f"Reading TMDB from: {tmdb_path}")
        tmdb_df = spark.read.parquet(tmdb_path)
        print(f"TMDB rows: {tmdb_df.count()}")
        
        print(f"Reading OMDB from: {omdb_path}")
        omdb_df = spark.read.parquet(omdb_path)
        print(f"OMDB rows: {omdb_df.count()}")
        
        # Inner join on tmdb_id
        print(f"\n=== Joining Data ===")
        enriched_df = tmdb_df.join(omdb_df, on="tmdb_id", how="inner")
        print(f"Joined rows: {enriched_df.count()}")
        
        # Select and arrange columns
        # Use explicit table references to handle any ambiguity
        enriched_df = enriched_df.select(
            "tmdb_id",
            "imdb_id",
            tmdb_df.title,
            tmdb_df.overview,
            tmdb_df.release_date,
            tmdb_df.vote_average,
            tmdb_df.vote_count,
            tmdb_df.popularity,
            tmdb_df.genre_ids,
            tmdb_df.original_language,
            tmdb_df.media_type,
            omdb_df.Director,
            omdb_df.Actors,
            omdb_df.Genre,
            omdb_df.Runtime,
            omdb_df.Rated,
            omdb_df.imdbRating,
            omdb_df.imdbVotes,
            omdb_df.BoxOffice,
            omdb_df.Metascore,
            omdb_df.Language,
            omdb_df.Country,
            omdb_df.Plot,
            omdb_df.Year,
            omdb_df.Released
        )
        
        # Add computed columns
        print(f"\n=== Adding Computed Columns ===")
        
        # rating_diff: TMDB vote_average - OMDB imdbRating
        enriched_df = enriched_df.withColumn(
            "rating_diff",
            when(
                col("vote_average").isNotNull() & col("imdbRating").isNotNull(),
                spark_round(col("vote_average") - col("imdbRating"), 2)
            ).otherwise(None)
        )
        
        # box_office_per_vote: BoxOffice / imdbVotes
        enriched_df = enriched_df.withColumn(
            "box_office_per_vote",
            when(
                col("BoxOffice").isNotNull() & col("imdbVotes").isNotNull() & (col("imdbVotes") > 0),
                spark_round(col("BoxOffice").cast("double") / col("imdbVotes"), 2)
            ).otherwise(None)
        )
        
        # popularity_score: (vote_average * 0.4) + (popularity * 0.3) + (imdbRating * 0.3)
        # Default nulls to 0
        enriched_df = enriched_df.withColumn(
            "popularity_score",
            spark_round(
                (coalesce(col("vote_average"), lit(0)) * 0.4) +
                (coalesce(col("popularity"), lit(0)) * 0.3) +
                (coalesce(col("imdbRating"), lit(0)) * 0.3),
                2
            )
        )
        
        print(f"Computed columns added:")
        print(f"  - rating_diff")
        print(f"  - box_office_per_vote")
        print(f"  - popularity_score")
        
        # Add content-based recommendations using shared genres and similar IMDb ratings
        print(f"\n=== Adding Movie Recommendations ===")
        candidates = enriched_df.select("tmdb_id", "title", "Genre", "imdbRating").collect()
        candidate_meta = []
        for row in candidates:
            genre_str = row[2]
            genres = set()
            if genre_str is not None:
                genres = {g.strip().lower() for g in genre_str.split(",") if g.strip()}
            rating_val = float(row[3]) if row[3] is not None else None
            candidate_meta.append({
                "tmdb_id": row[0],
                "title": row[1],
                "genres": genres,
                "rating": rating_val
            })

        def similar_titles(current_tmdb_id, current_genre, current_rating):
            if current_genre is None or current_rating is None:
                return "N/A"

            current_genres = {g.strip().lower() for g in current_genre.split(",") if g.strip()}
            if not current_genres:
                return "N/A"

            scored = []
            for candidate in candidate_meta:
                if candidate["tmdb_id"] == current_tmdb_id:
                    continue
                shared = current_genres.intersection(candidate["genres"])
                if not shared:
                    continue
                if candidate["rating"] is None:
                    continue
                score = (len(shared), -abs(current_rating - candidate["rating"]))
                scored.append((score, candidate["title"]))

            if not scored:
                return "N/A"

            scored.sort(key=lambda item: (-item[0][0], -item[0][1], item[1]))
            top_titles = [title for _, title in scored[:3]]
            return ", ".join(top_titles) if top_titles else "N/A"

        recommendation_udf = udf(similar_titles, StringType())
        enriched_df = enriched_df.withColumn(
            "recommended_similar",
            recommendation_udf(col("tmdb_id"), col("Genre"), col("imdbRating"))
        )
        print(f"Computed column added: recommended_similar")
        
        # Print schema
        print(f"\n=== Enriched Schema ===")
        enriched_df.printSchema()
        
        # Row count
        row_count = enriched_df.count()
        print(f"\nEnriched dataset rows: {row_count}")
        
        # Show key columns
        print(f"\n=== Enriched Data (All Rows - Key Columns) ===")
        enriched_df.select(
            "tmdb_id",
            "title",
            "vote_average",
            "imdbRating",
            "rating_diff",
            "popularity_score",
            "BoxOffice"
        ).show(row_count, truncate=False)
        
        # Save to Parquet
        print(f"\n=== Saving Enriched Data ===")
        print(f"Saving to: {output_dir}")
        enriched_df.write.mode("overwrite").parquet(output_dir)
        print("Enriched Parquet saved successfully.")
        
        print("\n=== Combination Complete ===")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
