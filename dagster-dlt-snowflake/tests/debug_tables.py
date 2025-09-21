from dagster_snowflake import SnowflakeResource
from dagster import asset
from pathlib import Path
import os

# Load environment variables
try:
    from dotenv import load_dotenv
    env_file = Path(__file__).parent.parent.parent / ".env"
    if env_file.exists():
        load_dotenv(env_file)
except ImportError:
    pass

@asset()
def debug_snowflake_tables(snowflake: SnowflakeResource) -> None:
    """
    Debug asset to check what tables exist in Snowflake
    """
    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        print("=== CHECKING SNOWFLAKE TABLES ===")

        # Check what schemas exist
        cursor.execute("SHOW SCHEMAS IN DATABASE dagster_db")
        schemas = cursor.fetchall()
        print("Available schemas:", [row[1] for row in schemas])

        # Check tables in mflix schema
        cursor.execute("SHOW TABLES IN SCHEMA mflix")
        tables = cursor.fetchall()
        print("Tables in mflix schema:", [row[1] for row in tables])

        # Check if there are any tables with 'mongodb' in the name
        cursor.execute("SHOW TABLES LIKE '%mongo%' IN SCHEMA mflix")
        mongodb_tables = cursor.fetchall()
        print("MongoDB-related tables:", [row[1] for row in mongodb_tables])

        # Check any tables with 'comment' in the name
        cursor.execute("SHOW TABLES LIKE '%comment%' IN SCHEMA mflix")
        comment_tables = cursor.fetchall()
        print("Comment-related tables:", [row[1] for row in comment_tables])

        # Check any tables with 'movie' in the name
        cursor.execute("SHOW TABLES LIKE '%movie%' IN SCHEMA mflix")
        movie_tables = cursor.fetchall()
        print("Movie-related tables:", [row[1] for row in movie_tables])