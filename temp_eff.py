import psycopg2
from environs import Env
from loguru import logger

# Nếu muốn dùng DLT pipeline có sẵn trong project:
# from utils.workflow_helper import document_state_resource
# import dlt

env = Env()
DATABASE_URL = env.str("DATA_PIPELINE_VBPLNEW_DATABASE_URL")


def rollback_null_eff_status_to_step_5():
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        with conn.cursor() as cur:
            # 1. Truy vấn các item_id bị NULL eff_status_id
            query_find_nulls = """
                SELECT item_id
                FROM "public"."documents"
                WHERE eff_status_id IS NULL;
            """
            cur.execute(query_find_nulls)
            rows = cur.fetchall()

            item_ids = [row[0] for row in rows]

            if not item_ids:
                logger.info("🎉 Không có văn bản nào bị NULL eff_status_id.")
                return

            logger.info(
                f"🔍 Tìm thấy {len(item_ids)} văn bản bị NULL eff_status. Bắt đầu chuyển về Bước 5..."
            )

            # 2. Xử lý chuyển đổi State về Bước 5

            # CÁCH A: Cập nhật trực tiếp bằng SQL (Giả định bạn có bảng document_state)
            # Lưu ý: Sửa lại tên bảng/cột cho khớp với schema thực tế của hệ thống
            query_update_state = """
                UPDATE "public"."document_state"
                SET workflow_id = 5,
                    start_time = NOW(),
                    end_time = NULL
                WHERE item_id = ANY(%s);
            """
            cur.execute(query_update_state, (item_ids,))
            conn.commit()
            logger.success("✅ Đã cập nhật database thành công bằng SQL.")

            # CÁCH B: Sử dụng dlt resource có sẵn trong hệ thống (Giống cách làm trong step_load_document_detail)
            # pipeline = dlt.pipeline(
            #     destination="postgres",
            #     dataset_name="public",
            #     pipeline_name="rollback_workflow"
            # )
            # pipeline.run(
            #     document_state_resource(
            #         workflow_id=5,
            #         item_ids=item_ids,
            #         start_time=datetime.now(),
            #         end_time=datetime.now(),
            #     )
            # )
            # logger.success("✅ Đã đẩy state vào DLT Pipeline thành công.")

    except Exception as e:
        logger.error(f"❌ Lỗi trong quá trình xử lý: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    rollback_null_eff_status_to_step_5()
