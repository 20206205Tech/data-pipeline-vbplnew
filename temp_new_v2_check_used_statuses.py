import psycopg2
from environs import Env

env = Env()
DATABASE_URL = env.str("DATA_PIPELINE_VBPLNEW_DATABASE_URL")


def check_used_statuses():
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT d.eff_status_id, s.name, s.code
            FROM "public"."documents" d
            LEFT JOIN "public"."dim_eff_status" s ON d.eff_status_id = s.id
        """
        )
        rows = cur.fetchall()
        for row in rows:
            print(row)
    conn.close()


if __name__ == "__main__":
    check_used_statuses()
