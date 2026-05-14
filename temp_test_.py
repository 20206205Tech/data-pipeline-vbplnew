import io
import sys

import psycopg2
from loguru import logger

import env

# Set encoding for Windows terminal
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


def check_database():
    try:
        conn = psycopg2.connect(env.DATABASE_URL)
        with conn.cursor() as cur:
            # List all tables
            logger.info("Listing all tables in 'public' schema...")
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            tables = cur.fetchall()
            for table in tables:
                print(f"Table: {table[0]}")

            # Check workflows table
            logger.info("Checking 'workflows' table...")
            cur.execute(
                "SELECT id, code, description FROM public.workflows ORDER BY id"
            )
            rows = cur.fetchall()
            for row in rows:
                print(f"Step {row[0]}: {row[1]} - {row[2]}")

            # Check documents table structure
            logger.info("\nChecking 'documents' table structure...")
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'documents'
                ORDER BY ordinal_position
            """
            )
            cols = cur.fetchall()
            for col in cols:
                print(f"Column: {col[0]} ({col[1]})")

            # Check dim_eff_status table structure
            logger.info("\nChecking 'dim_eff_status' table structure...")
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'dim_eff_status'
            """
            )
            cols = cur.fetchall()
            for col in cols:
                print(f"Column: {col[0]} ({col[1]})")

            # Check JOIN results
            logger.info("\nChecking JOIN between 'documents' and 'dim_eff_status'...")
            cur.execute(
                """
                SELECT d.item_id, d.title, s.name as status_name
                FROM public.documents d
                LEFT JOIN public.dim_eff_status s ON d.eff_status_id = s.id
                LIMIT 5
            """
            )
            rows = cur.fetchall()
            for row in rows:
                print(f"ID: {row[0]} | Title: {row[1][:50]}... | Status: {row[2]}")

        conn.close()
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    check_database()
