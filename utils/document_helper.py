import psycopg2
from loguru import logger

# Danh sách các trạng thái cần bỏ qua
STATUS_TO_SKIP = [
    #   'Còn hiệu lực',
    "Không còn phù hợp",
    # "Hết hiệu lực một phần",
    #   'Ngưng hiệu lực một phần',
    "Ngưng hiệu lực",
    #   'Chưa có hiệu lực',
    "Hết hiệu lực toàn bộ",
]


def is_document_invalid(status: str) -> bool:
    """
    Kiểm tra xem văn bản có thuộc diện hết hiệu lực/cần bỏ qua hay không.
    """
    if not status:
        return False
    return status.strip() in STATUS_TO_SKIP


def get_document_statuses_from_db(conn, item_ids: list) -> dict:
    """
    Lấy trạng thái (name từ bảng dim_eff_status) cho nhiều item cùng lúc bằng cách JOIN với bảng documents.
    Trả về: Dictionary { 'item_id': 'status_name' }
    """
    if not item_ids:
        return {}

    str_item_ids = tuple(str(id) for id in item_ids)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.item_id, s.name
                FROM "public"."documents" d
                LEFT JOIN "public"."dim_eff_status" s ON d.eff_status_id = s.id
                WHERE d.item_id IN %s
                """,
                (str_item_ids,),
            )
            rows = cur.fetchall()
            return {str(row[0]): row[1] for row in rows}

    except psycopg2.errors.UndefinedTable:
        conn.rollback()
    except Exception as e:
        logger.debug(f"Lỗi truy vấn status từ bảng documents/dim_eff_status: {e}")
        conn.rollback()

    return {}
