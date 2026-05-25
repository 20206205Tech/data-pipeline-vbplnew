import psycopg2

import env  # Import module chứa DATABASE_URL của bạn


def export_schema_and_data_to_txt():
    conn = None
    try:
        # Kết nối tới PostgreSQL sử dụng URL từ biến env
        conn = psycopg2.connect(env.DATABASE_URL)
        cur = conn.cursor()

        # Bước 1: Lấy danh sách tất cả các bảng trong public schema
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """
        )
        tables = cur.fetchall()

        # Bước 2: Xử lý dữ liệu và ghi file
        with open("temp_database.txt", "w", encoding="utf-8") as f:
            for (table_name,) in tables:
                f.write(f"\n{'='*50}\n")
                f.write(f"[BẢNG]: {table_name}\n")
                f.write(f"{'='*50}\n")

                # Lấy toàn bộ dữ liệu của bảng
                # LƯU Ý: Thêm LIMIT 10 để tránh treo máy nếu bảng quá lớn.
                # Bạn có thể xóa "LIMIT 10" nếu chắc chắn muốn xuất TOÀN BỘ dữ liệu.
                query = f'SELECT * FROM "{table_name}" LIMIT 10;'
                cur.execute(query)

                # Lấy tên cột từ cursor.description
                col_names = [desc[0] for desc in cur.description]
                f.write(f"  - Các cột: {', '.join(col_names)}\n\n")

                # Lấy dữ liệu (rows)
                rows = cur.fetchall()
                f.write("  - Dữ liệu:\n")

                if not rows:
                    f.write("    (Bảng không có dữ liệu)\n")
                else:
                    for row in rows:
                        f.write(f"    {row}\n")

        print("Successfully exported database schema and data to temp_database.txt")

    except Exception as e:
        print(f"Error connecting or querying: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()


if __name__ == "__main__":
    export_schema_and_data_to_txt()
