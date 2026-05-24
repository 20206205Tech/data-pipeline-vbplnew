# # # import psycopg2
# # # from environs import Env
# # # from loguru import logger

# # # # Nếu muốn dùng DLT pipeline có sẵn trong project:
# # # # from utils.workflow_helper import document_state_resource
# # # # import dlt

# # # env = Env()
# # # DATABASE_URL = env.str("DATA_PIPELINE_VBPLNEW_DATABASE_URL")


# # # def rollback_null_eff_status_to_step_5():
# # #     conn = None
# # #     try:
# # #         conn = psycopg2.connect(DATABASE_URL)
# # #         with conn.cursor() as cur:
# # #             # 1. Truy vấn các item_id bị NULL eff_status_id
# # #             query_find_nulls = """
# # #                 SELECT item_id
# # #                 FROM "public"."documents"
# # #                 WHERE eff_status_id IS NULL;
# # #             """
# # #             cur.execute(query_find_nulls)
# # #             rows = cur.fetchall()

# # #             item_ids = [row[0] for row in rows]

# # #             if not item_ids:
# # #                 logger.info("🎉 Không có văn bản nào bị NULL eff_status_id.")
# # #                 return

# # #             logger.info(
# # #                 f"🔍 Tìm thấy {len(item_ids)} văn bản bị NULL eff_status. Bắt đầu chuyển về Bước 5..."
# # #             )

# # #             # 2. Xử lý chuyển đổi State về Bước 5

# # #             # CÁCH A: Cập nhật trực tiếp bằng SQL (Giả định bạn có bảng document_state)
# # #             # Lưu ý: Sửa lại tên bảng/cột cho khớp với schema thực tế của hệ thống
# # #             query_update_state = """
# # #                 UPDATE "public"."document_state"
# # #                 SET workflow_id = 5,
# # #                     start_time = NOW(),
# # #                     end_time = NULL
# # #                 WHERE item_id = ANY(%s);
# # #             """
# # #             cur.execute(query_update_state, (item_ids,))
# # #             conn.commit()
# # #             logger.success("✅ Đã cập nhật database thành công bằng SQL.")

# # #             # CÁCH B: Sử dụng dlt resource có sẵn trong hệ thống (Giống cách làm trong step_load_document_detail)
# # #             # pipeline = dlt.pipeline(
# # #             #     destination="postgres",
# # #             #     dataset_name="public",
# # #             #     pipeline_name="rollback_workflow"
# # #             # )
# # #             # pipeline.run(
# # #             #     document_state_resource(
# # #             #         workflow_id=5,
# # #             #         item_ids=item_ids,
# # #             #         start_time=datetime.now(),
# # #             #         end_time=datetime.now(),
# # #             #     )
# # #             # )
# # #             # logger.success("✅ Đã đẩy state vào DLT Pipeline thành công.")

# # #     except Exception as e:
# # #         logger.error(f"❌ Lỗi trong quá trình xử lý: {e}")
# # #         if conn:
# # #             conn.rollback()
# # #     finally:
# # #         if conn:
# # #             conn.close()


# # # if __name__ == "__main__":
# # #     rollback_null_eff_status_to_step_5()


# # import psycopg2
# # from environs import Env
# # from loguru import logger

# # env = Env()
# # DATABASE_URL = env.str("DATA_PIPELINE_VBPLNEW_DATABASE_URL")

# # def unlock_step_5_tasks():
# #     conn = psycopg2.connect(DATABASE_URL)
# #     try:
# #         with conn.cursor() as cur:
# #             query = """
# #                 UPDATE "public"."document_state"
# #                 SET start_time = NULL
# #                 WHERE workflow_id = 5
# #                   AND start_time IS NOT NULL
# #                   AND end_time IS NULL;
# #             """
# #             cur.execute(query)
# #             conn.commit()
# #             logger.success(f"✅ Đã mở khóa thành công {cur.rowcount} tasks. Sẵn sàng để crawl!")
# #     except Exception as e:
# #         logger.error(f"❌ Lỗi: {e}")
# #         conn.rollback()
# #     finally:
# #         conn.close()

# # if __name__ == "__main__":
# #     unlock_step_5_tasks()

# import psycopg2
# from environs import Env
# from loguru import logger

# # Import config để lấy ID tự động nếu bạn không muốn hardcode số 4
# # import workflow_config

# env = Env()
# DATABASE_URL = env.str("DATA_PIPELINE_VBPLNEW_DATABASE_URL")

# def rollback_to_trigger_crawl():
#     conn = psycopg2.connect(DATABASE_URL)
#     try:
#         with conn.cursor() as cur:
#             # Nếu ID của step_load_document_list không phải là 4, bạn có thể thay thế bằng:
#             # previous_step_id = workflow_config.STEP_LOAD_DOCUMENT_LIST.id
#             previous_step_id = 4
#             current_step_id = 5

#             query = """
#                 UPDATE "public"."document_state"
#                 SET workflow_id = %s,
#                     start_time = NOW(),
#                     end_time = NOW()
#                 WHERE workflow_id = %s;
#             """
#             cur.execute(query, (previous_step_id, current_step_id))
#             conn.commit()

#             logger.success(f"✅ Đã lùi {cur.rowcount} tasks về workflow_id = {previous_step_id} (Trạng thái hoàn thành). Sẵn sàng cho Bước {current_step_id}!")

#     except Exception as e:
#         logger.error(f"❌ Lỗi thực thi SQL: {e}")
#         conn.rollback()
#     finally:
#         conn.close()

# if __name__ == "__main__":
#     rollback_to_trigger_crawl()


import psycopg2
from environs import Env
from loguru import logger

env = Env()
DATABASE_URL = env.str("DATA_PIPELINE_VBPLNEW_DATABASE_URL")


def fix_workflow_state_for_crawl():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            # Câu lệnh này sẽ làm 3 việc tự động:
            # 1. Tìm các item_id bị thiếu eff_status_id
            # 2. Tìm parent_id chuẩn xác của bước crawl detail
            # 3. Cập nhật state chuẩn để hàm fetch_and_lock_pending_tasks nhận diện được
            query = """
                UPDATE "public"."document_state"
                SET workflow_id = (
                        SELECT parent_id
                        FROM "public"."workflows"
                        WHERE code = 'step_crawl_document_detail'
                    ),
                    start_time = NOW() - INTERVAL '1 minute',
                    end_time = NOW()
                WHERE item_id IN (
                    SELECT item_id
                    FROM "public"."documents"
                    WHERE eff_status_id IS NULL
                );
            """
            cur.execute(query)
            conn.commit()

            logger.success(
                f"✅ Đã reset thành công {cur.rowcount} văn bản về đúng state cha của bước Crawl Detail."
            )

    except Exception as e:
        logger.error(f"❌ Lỗi thực thi SQL: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    fix_workflow_state_for_crawl()
