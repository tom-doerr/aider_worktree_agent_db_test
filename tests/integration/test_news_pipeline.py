import pytest
import time
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import DictCursor

@pytest.fixture
def db_connection_fixture():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres"
    )
    yield conn
    conn.close()

def test_full_pipeline_integration(db_connection_fixture):
    """Test the full news processing pipeline from scraping to commentary generation."""
    
    # 1. Verify scraper service is running and inserting data
    with db_connection_fixture.cursor(cursor_factory=DictCursor) as cur:
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
    with db_connection_fixture.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("SELECT COUNT(*) FROM parsed_articles WHERE created_at > %s", 
                   (datetime.now() - timedelta(minutes=2),))
        assert cur.fetchone()[0] > 0, "No new parsed articles found"

    # 3. Verify categorized articles
    with db_connection_fixture.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT COUNT(*) FROM categorized_articles 
            WHERE created_at > %s AND category IS NOT NULL
        """, (datetime.now() - timedelta(minutes=2),))
        assert cur.fetchone()[0] > 0, "No categorized articles found"

    # 4. Verify commentary generation
    with db_connection_fixture.cursor(cursor_factory=DictCursor) as cur:
        cur.execute("""
            SELECT COUNT(*) FROM commentary_articles 
            WHERE created_at > %s AND content IS NOT NULL
        """, (datetime.now() - timedelta(minutes=2),))
        assert cur.fetchone()[0] > 0, "No commentary articles generated"
