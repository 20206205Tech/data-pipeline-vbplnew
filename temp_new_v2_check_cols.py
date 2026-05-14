# import psycopg2
# import env

# def check_cols():
#     conn = psycopg2.connect(env.DATABASE_URL)
#     cur = conn.cursor()
#     cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'documents'")
#     cols = [c[0] for c in cur.fetchall()]
#     print(f"Columns in documents: {cols}")
#     conn.close()

# if __name__ == "__main__":
#     check_cols()
