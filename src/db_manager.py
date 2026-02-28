import os
import psycopg
from psycopg.types.json import Json
from dotenv import load_dotenv
from typing import Dict, Any, List

from logger import get_logger
logger = get_logger(__name__)

load_dotenv(encoding='utf-8')


class DBManager:
    """
    Handles all database operations. Uses Psycopg 3 for PostgreSQL interaction.
    """

    def __init__(self):
        self.conn_info = (
            f"host={os.getenv('POSTGRES_HOST')} "
            f"dbname={os.getenv('POSTGRES_DB')} "
            f"user={os.getenv('POSTGRES_USER')} "
            f"password={os.getenv('POSTGRES_PASSWORD')} "
            f"port={os.getenv('POSTGRES_PORT')}"
        )

    def save_questions(self, items: List[Dict[str, Any]]):
        """
        Saves a list of questions into the database.
        Implements UPSERT logic to prevent duplicates.
        """
        try:
            with psycopg.connect(self.conn_info) as conn:
                with conn.cursor() as cur:
                    for item in items:
                        # 1. Save Question
                        cur.execute("""
                            INSERT INTO fact_questions (
                                question_id, title, body_markdown, creation_date, 
                                is_answered, view_count, answer_count, score, tags_raw
                            )
                            VALUES (%s, %s, %s, to_timestamp(%s), %s, %s, %s, %s, %s)
                            ON CONFLICT (question_id) DO UPDATE SET
                                title = EXCLUDED.title,
                                view_count = EXCLUDED.view_count,
                                answer_count = EXCLUDED.answer_count,
                                score = EXCLUDED.score,
                                is_answered = EXCLUDED.is_answered,
                                last_loaded_at = NOW();
                        """, (
                            item['question_id'],
                            item.get('title'),
                            item.get('body_markdown'),
                            item.get('creation_date'),
                            item.get('is_answered'),
                            item.get('view_count'),
                            item.get('answer_count'),
                            item.get('score'),
                            Json(item.get('tags', []))
                        ))

                        # 2. Save Tags
                        cur.execute("DELETE FROM bridge_question_tags WHERE question_id = %s", (item['question_id'],))

                        tags = item.get('tags', [])
                        for tag in tags:
                            cur.execute("""
                                INSERT INTO bridge_question_tags (question_id, tag_name)
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING;
                            """, (item['question_id'], tag))

                    conn.commit()
            logger.info(f"Successfully saved {len(items)} questions to the database.")

        except Exception as e:
            logger.error(f"Database error: {e}")


if __name__ == "__main__":
    db = DBManager()
    dummy_data = [{
        "question_id": 999999,
        "title": "Test Question",
        "creation_date": 1708612345,
        "is_answered": False,
        "view_count": 10,
        "answer_count": 1,
        "score": 5,
        "tags": ["python", "test"]
    }]
    db.save_questions(dummy_data)