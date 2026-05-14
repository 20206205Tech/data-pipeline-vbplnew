import psycopg2

import env  # Import module chứa DATABASE_URL của bạn


def export_schema_to_txt():
    conn = None
    try:
        # Kết nối tới PostgreSQL sử dụng URL từ biến env
        conn = psycopg2.connect(env.DATABASE_URL)
        cur = conn.cursor()

        # Câu lệnh SQL lấy tên bảng và cột
        query = """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position;
        """

        cur.execute(query)
        rows = cur.fetchall()

        # Xử lý dữ liệu và ghi file
        with open("temp_database.txt", "w", encoding="utf-8") as f:
            current_table = ""
            for table, column in rows:
                if table != current_table:
                    f.write(f"\n[BẢNG]: {table}\n")
                    current_table = table
                f.write(f"  - Cột: {column}\n")

        print("Successfully exported database schema to temp_database.txt")

    except Exception as e:
        print(f"Error connecting or querying: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()


if __name__ == "__main__":
    export_schema_to_txt()
