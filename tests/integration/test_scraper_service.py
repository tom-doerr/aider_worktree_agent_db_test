import os
import pytest
import time
from datetime import datetime, timedelta  # pylint: disable=unused-import
import psycopg2
from psycopg2.extras import DictCursor


def get_db_connection():
    """Helper function to create DB connection with env vars."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        connect_timeout=3,
    )


@pytest.fixture(name="db_conn")
def db_connection_fixture():
    conn = get_db_connection()
    yield conn
    conn.close()


def test_scraper_service_runs(db_conn):
    """Verify scraper service is running and inserting data to raw_articles."""
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
