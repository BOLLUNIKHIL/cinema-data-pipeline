import os
import sys
import json
import time
import datetime
from typing import List, Dict, Any

import requests
from dotenv import load_dotenv


def load_keys():
    # Load .env from project root (one level up from scripts/)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    env_path = os.path.join(project_root, ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"Loaded .env from {env_path}")
    else:
        print(f"Warning: .env not found at {env_path}; falling back to environment variables")

    tmdb_key = os.getenv("TMDB_API_KEY") or os.getenv("TMDB_KEY")
    omdb_key = os.getenv("OMDB_API_KEY") or os.getenv("OMDB_KEY")

    if not tmdb_key:
        print("ERROR: TMDB API key not found. Please set TMDB_API_KEY in .env or environment.")
        sys.exit(1)
    if not omdb_key:
        print("ERROR: OMDB API key not found. Please set OMDB_API_KEY in .env or environment.")
        sys.exit(1)

    return tmdb_key, omdb_key


def fetch_trending(tmdb_key: str) -> Dict[str, Any]:
    url = "https://api.themoviedb.org/3/trending/movie/day"
    params = {"api_key": tmdb_key}
    print("Fetching TMDB trending movies...")
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    print(f"Fetched {len(data.get('results', []))} trending items")
    return data


def fetch_imdb_id_for_movie(tmdb_id: int, tmdb_key: str) -> str | None:
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/external_ids"
    params = {"api_key": tmdb_key}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        imdb_id = data.get("imdb_id")
        return imdb_id
    except requests.RequestException as e:
        print(f"Warning: failed to fetch external ids for TMDB id {tmdb_id}: {e}")
        return None


def fetch_omdb_details(imdb_id: str, omdb_key: str) -> Dict[str, Any] | None:
    url = "http://www.omdbapi.com/"
    params = {"apikey": omdb_key, "i": imdb_id}
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("Response") == "False":
            print(f"OMDB returned error for {imdb_id}: {data.get('Error')}")
            return None
        return data
    except requests.RequestException as e:
        print(f"Warning: failed to fetch OMDB for {imdb_id}: {e}")
        return None


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def save_json(path: str, obj: Any):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    print(f"Saved {path}")


def main():
    tmdb_key, omdb_key = load_keys()

    today = datetime.date.today().isoformat()

    # Paths
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    tmdb_dir = os.path.join(repo_root, "data", "raw", "cinema", "tmdb")
    omdb_dir = os.path.join(repo_root, "data", "raw", "cinema", "omdb")
    ensure_dir(tmdb_dir)
    ensure_dir(omdb_dir)

    tmdb_out = os.path.join(tmdb_dir, f"trending_{today}.json")
    omdb_out = os.path.join(omdb_dir, f"details_{today}.json")

    try:
        trending = fetch_trending(tmdb_key)
    except requests.RequestException as e:
        print(f"ERROR: Failed to fetch trending movies: {e}")
        sys.exit(1)

    save_json(tmdb_out, trending)

    omdb_results: List[Dict[str, Any]] = []

    for idx, movie in enumerate(trending.get("results", []), start=1):
        tmdb_id = movie.get("id")
        title = movie.get("title") or movie.get("name")
        print(f"({idx}) Processing: {title} (TMDB id: {tmdb_id})")
        if not tmdb_id:
            print("  Skipping: no TMDB id")
            continue

        imdb_id = fetch_imdb_id_for_movie(tmdb_id, tmdb_key)
        if not imdb_id:
            print("  No IMDB id found; skipping OMDB lookup")
            continue

        print(f"  Found IMDB id: {imdb_id}; fetching OMDB details...")
        details = fetch_omdb_details(imdb_id, omdb_key)
        if details:
            details_record = {"tmdb_id": tmdb_id, "imdb_id": imdb_id, "omdb": details}
            omdb_results.append(details_record)

        # be polite with API rate limits
        time.sleep(0.25)

    save_json(omdb_out, omdb_results)
    print("Done.")


if __name__ == "__main__":
    main()
