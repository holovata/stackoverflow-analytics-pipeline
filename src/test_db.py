import os
import psycopg
from dotenv import load_dotenv

load_dotenv(encoding='utf-8')


def test_connection():
    try:
        conn_info = (
            f"host={os.getenv('POSTGRES_HOST')} "
            f"dbname={os.getenv('POSTGRES_DB')} "
            f"user={os.getenv('POSTGRES_USER')} "
            f"password={os.getenv('POSTGRES_PASSWORD')} "
            f"port={os.getenv('POSTGRES_PORT')}"
        )

        with psycopg.connect(conn_info) as conn:
            print("Python (Psycopg 3) successfully connected to PostgreSQL!")

            with conn.cursor() as cur:
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                tables = cur.fetchall()
                print(f"Tables found in DB: {tables}")

    except Exception as e:
        print(f"Connection failed: {e}")


if __name__ == "__main__":
    test_connection()