import psycopg
import os
from dotenv import load_dotenv

load_dotenv()


def check_my_data():
    conn_info = f"host={os.getenv('POSTGRES_HOST')} dbname={os.getenv('POSTGRES_DB')} user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')} port={os.getenv('POSTGRES_PORT')}"

    with psycopg.connect(conn_info) as conn:
        with conn.cursor() as cur:
            # 1. Total questions
            cur.execute("SELECT COUNT(*) FROM fact_questions;")
            print(f"Total Questions: {cur.fetchone()[0]}")

            # 2. Top 5 tags in the bridge table
            print("\nTop 5 Related Tags:")
            cur.execute("""
                SELECT tag_name, COUNT(*) as count 
                FROM bridge_question_tags 
                GROUP BY tag_name 
                ORDER BY count DESC 
                LIMIT 5;
            """)
            for row in cur.fetchall():
                print(f"- {row[0]}: {row[1]}")


if __name__ == "__main__":
    check_my_data()