##db.py
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from osdudb.env
load_dotenv("backend/osdudb.env")

_conn = None

def get_conn():
    global _conn
    if _conn is None:
        conn_str = f"postgresql://{os.getenv('OSDU_DB_USER')}:{os.getenv('OSDU_DB_PASSWORD')}@" \
                   f"{os.getenv('OSDU_DB_HOST')}:{os.getenv('OSDU_DB_PORT')}/{os.getenv('OSDU_DB_NAME')}"
        print(f"🔗 Connecting using: {conn_str}")

        _conn = psycopg2.connect(
            dbname=os.getenv("OSDU_DB_NAME"),
            user=os.getenv("OSDU_DB_USER"),
            password=os.getenv("OSDU_DB_PASSWORD"),
            host=os.getenv("OSDU_DB_HOST"),
            port=os.getenv("OSDU_DB_PORT")
        )
    return _conn
print("Connecting to DB:", os.getenv("OSDU_DB_NAME"), os.getenv("OSDU_DB_USER"))
