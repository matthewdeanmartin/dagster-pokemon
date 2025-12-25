import sqlite3
import pandas as pd
import requests
import io

# Constants
DEFAULT_DB_PATH = "pokemon_movies.db"
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_Pok%C3%A9mon_films"


def scrape_pokemon_data(url: str = WIKI_URL) -> pd.DataFrame:
    """
    Fetches data using requests to handle headers, then parses with Pandas.
    """
    # 1. Define headers to look like a legitimate browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
    }

    # 2. Fetch the raw HTML content
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an error if the download failed (403, 404, etc.)

    # 3. Parse the HTML string directly
    # We wrap response.text in StringIO to avoid pandas warnings about memory
    dfs = pd.read_html(io.StringIO(response.text))

    target_df = pd.DataFrame()
    for df in dfs:
        if "English title" in df.columns and "Japanese release date" in df.columns:
            target_df = df
            break

    if target_df.empty:
        raise ValueError("Could not find the Pokemon movies table on Wikipedia.")

    clean_df = target_df[["English title", "Japanese release date"]].dropna()
    clean_df.columns = ["title", "release_date"]

    return clean_df.astype(str)


def sync_movies_to_db(new_data_df: pd.DataFrame, db_path: str = DEFAULT_DB_PATH) -> tuple[int, pd.DataFrame]:
    """
    Pure database logic. Unchanged from previous version.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS movies
                   (
                       title
                       TEXT
                       PRIMARY
                       KEY,
                       release_date
                       TEXT
                   )
                   """)
    conn.commit()

    existing_movies = pd.read_sql("SELECT title FROM movies", conn)
    existing_titles = set(existing_movies["title"].tolist())

    new_movies_df = new_data_df[
        ~new_data_df["title"].isin(existing_titles)
    ]

    new_count = len(new_movies_df)

    if new_count > 0:
        new_movies_df.to_sql("movies", conn, if_exists="append", index=False)

    conn.close()

    return new_count, new_movies_df


# --- Debug Block ---
if __name__ == "__main__":
    print("--- Debugging Logic ---")
    print(f"Fetching data from {WIKI_URL} with spoofed headers...")
    try:
        df = scrape_pokemon_data()
        print(f"Success! Scraped {len(df)} rows.")
        print(df.head())
    except Exception as e:
        print(f"Failed: {e}")