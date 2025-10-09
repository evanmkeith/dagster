import pandas as pd
import dagster as dg
from pathlib import Path
import os
import requests

@dg.asset(group_name="tmb_trending", compute_kind="TMDB API")
def tmdb_trending_movies() -> pd.DataFrame:
    """Fetches trending movies from TMDB API and returns as a pandas DataFrame."""
    trending_url = "https://api.themoviedb.org/3/trending/movie/day?language=en-US"

