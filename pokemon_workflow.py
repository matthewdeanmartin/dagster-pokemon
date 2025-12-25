import pandas as pd
from dagster import asset, Definitions, MetadataValue

# Import the logic from the other file
import pokemon_logic

@asset
def pokemon_movies_data() -> pd.DataFrame:
    """
    Wraps the scraping logic.
    """
    return pokemon_logic.scrape_pokemon_data()


@asset
def movies_database(context, pokemon_movies_data: pd.DataFrame) -> None:
    """
    Wraps the database sync logic.
    Maps return values from the logic layer into Dagster metadata/logs.
    """
    # Call the pure function
    new_count, new_movies_df = pokemon_logic.sync_movies_to_db(pokemon_movies_data)

    # Handle Dagster-specific logging
    if new_count > 0:
        context.log.info(f"Added {new_count} new movies to the database.")
    else:
        context.log.info("No new movies found. Database is up to date.")

    # Handle Dagster-specific metadata
    context.add_output_metadata({
        "total_rows_scraped": len(pokemon_movies_data),
        "new_movies_added": new_count,
        "preview_new_movies": MetadataValue.md(new_movies_df.head().to_markdown())
    })

# Define the Dagster code location
defs = Definitions(
    assets=[pokemon_movies_data, movies_database],
)