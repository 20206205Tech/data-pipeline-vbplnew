import psycopg2

import env


def check_status():
    conn = psycopg2.connect(env.DATABASE_URL)
    with conn.cursor() as cur:
        cur.execute('SELECT id, name, code FROM "public"."dim_eff_status"')
        rows = cur.fetchall()
        for row in rows:
            print(row)
    conn.close()


if __name__ == "__main__":
    check_status()
