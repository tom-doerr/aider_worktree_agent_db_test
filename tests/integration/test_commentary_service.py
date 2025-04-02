import os
import pytest
from datetime import datetime, timedelta
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


def test_commentary_service_runs(db_conn):
    """Verify commentary service generates commentary articles."""
    with db_conn.cursor(cursor_factory=DictCursor) as cur:
        # First ensure there are categorized articles to use
        cur.execute("SELECT COUNT(*) FROM categorized_articles")
        if cur.fetchone()[0] == 0:
            pytest.skip("No categorized articles available for commentary")

        # Check for commentary articles created recently
        cur.execute(
            """
            SELECT COUNT(*) FROM commentary_articles 
            WHERE created_at > %s AND content IS NOT NULL
            """,
            (datetime.now() - timedelta(minutes=2)),
        )
        assert cur.fetchone()[0] > 0, "Commentary service did not create commentary articles"
