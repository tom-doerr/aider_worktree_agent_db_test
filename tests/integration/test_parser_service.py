import os
import pytest
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import DictCursor


def get_db_connection():
    """Helper function to create DB connection with env vars."""
    try:
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            database=os.getenv("POSTGRES_DB", "postgres"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            connect_timeout=3,
        )
    except psycopg2.OperationalError as e:
        pytest.skip(f"Could not connect to database: {e}")


@pytest.fixture(name="db_conn")
def db_connection_fixture():
    conn = get_db_connection()
    yield conn
    conn.close()


def test_parser_service_runs(db_conn):
    """Verify parser service is processing raw articles into parsed_articles."""
    with db_conn.cursor(cursor_factory=DictCursor) as cur:
        # First ensure there are raw articles to parse
        cur.execute("SELECT COUNT(*) FROM raw_articles")
        if cur.fetchone()[0] == 0:
            pytest.skip("No raw articles available to parse")

        # Check for parsed articles created recently
        cur.execute(
            "SELECT COUNT(*) FROM parsed_articles WHERE created_at > %s",
            (datetime.now() - timedelta(minutes=2),),
        )
        assert cur.fetchone()[0] > 0, "Parser service did not create parsed articles"
