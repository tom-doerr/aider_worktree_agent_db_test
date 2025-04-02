import os
import pytest
import time
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import OperationalError


def get_db_connection():
    """Helper function to create DB connection with timeout and env vars."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            database=os.getenv("POSTGRES_DB", "postgres"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            connect_timeout=3,
        )
        return conn
    except OperationalError as e:
        pytest.skip(f"Skipping test - could not connect to database: {e}")


@pytest.fixture(name="db_conn")
def db_connection_fixture():
    conn = get_db_connection()
    yield conn
    conn.close()


def test_full_pipeline_integration(db_conn):
    """Test the full news processing pipeline from scraping to commentary generation."""

    # 1. Verify scraper service is running and inserting data
    with db_conn.cursor(cursor_factory=DictCursor) as cur:
        # Get initial count
        cur.execute("SELECT COUNT(*) FROM raw_articles")
        initial_count = cur.fetchone()[0]

        # Wait for scraper to run (assuming it runs every 60s)
        time.sleep(65)

        # Verify new data was added
        cur.execute("SELECT COUNT(*) FROM raw_articles")
        new_count = cur.fetchone()[0]
        assert new_count > initial_count, "Scraper did not add new articles"

    # 2. Verify parsed articles exist
    with db_conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            "SELECT COUNT(*) FROM parsed_articles WHERE created_at > %s",
            (datetime.now() - timedelta(minutes=2),),
        )
        assert cur.fetchone()[0] > 0, "No new parsed articles found"

    # 3. Verify categorized articles
    with db_conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM categorized_articles 
            WHERE created_at > %s AND category IS NOT NULL
        """,
            (datetime.now() - timedelta(minutes=2),),
        )
        assert cur.fetchone()[0] > 0, "No categorized articles found"

    # 4. Verify commentary generation
    with db_conn.cursor(cursor_factory=DictCursor) as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM commentary_articles 
            WHERE created_at > %s AND content IS NOT NULL
        """,
            (datetime.now() - timedelta(minutes=2),),
        )
        assert cur.fetchone()[0] > 0, "No commentary articles generated"
