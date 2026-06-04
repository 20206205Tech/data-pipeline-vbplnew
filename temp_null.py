import sys
from datetime import datetime
from typing import List, Tuple

import dlt
import plotext as plt
from loguru import logger

import workflow_config
from utils.config_by_path import ConfigByPath
from utils.workflow_helper import (
    get_document_eff_status_summary,
    get_document_state_count_by_workflow,
    log_error_workflow_state,
)

# Reconfigure stdout to use UTF-8 encoding to prevent UnicodeEncodeError on Windows
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

config_by_path = ConfigByPath(__file__)
PATH_FOLDER_OUTPUT = config_by_path.PATH_FOLDER_OUTPUT


def get_null_eff_status_item_ids(pipeline: dlt.Pipeline) -> List[str]:
    """
    Truy vấn danh sách item_id của các document có eff_status_id là NULL.
    """
    query = """
        SELECT item_id
        FROM public.documents
        WHERE eff_status_id IS NULL;
    """
    try:
        with pipeline.sql_client() as client:
            results = client.execute_sql(query)
            return [row[0] for row in results] if results else []
    except Exception as e:
        logger.error(f"Lỗi khi truy vấn danh sách document NULL: {e}")
        return []


def get_null_document_state_count_by_workflow(
    pipeline: dlt.Pipeline,
) -> List[Tuple[int, int]]:
    """
    Thống kê các item NULL đó ở các workflow nào tương tự như get_document_state_count_by_workflow.
    Vẽ biểu đồ phân bố và log chi tiết.
    """
    query = """
        SELECT ds.workflow_id, COUNT(*)
        FROM "public"."document_state" ds
        JOIN "public"."documents" d ON ds.item_id = d.item_id
        WHERE d.eff_status_id IS NULL
        GROUP BY ds.workflow_id
        ORDER BY ds.workflow_id ASC;
    """

    try:
        with pipeline.sql_client() as client:
            results = client.execute_sql(query)

            if not results:
                logger.info(
                    "Không có document NULL nào trong document_state để hiển thị."
                )
                return []

            logger.success("Đã lấy thành công thống kê document NULL theo workflow")

            # Chuẩn bị dữ liệu vẽ biểu đồ
            workflow_ids = [str(row[0]) for row in results]
            counts = [row[1] for row in results]

            plt.clear_figure()
            plt.bar(workflow_ids, counts)

            plt.title("Thong Ke So Luong Document NULL Theo Workflow ID")
            plt.xlabel("Workflow ID")
            plt.ylabel("So Luong NULL (Count)")

            plt.plotsize(80, 25)
            plt.theme("clear")
            plt.show()

            for workflow_id, count in results:
                logger.info(f"Workflow ID: {workflow_id}, Số lượng NULL: {count}")

            return results

    except Exception as e:
        if "does not exist" in str(e):
            logger.warning(
                "Bảng public.document_state hoặc public.documents chưa được tạo."
            )
            return []
        logger.error(f"Lỗi database khi lấy thống kê workflow của tài liệu NULL: {e}")
        raise


def main():
    pipeline = dlt.pipeline(
        destination="postgres",
        dataset_name="public",
        pipeline_name=config_by_path.NAME,
    )

    # 1. Hiển thị tổng quan số lượng tài liệu có eff_status_id là NULL
    logger.info("--- Thống kê tổng quan documents ---")
    get_document_eff_status_summary(pipeline)

    # 2. Thống kê chi tiết các document NULL đó đang ở workflow nào
    logger.info("--- Thống kê chi tiết documents NULL theo từng workflow ---")
    get_null_document_state_count_by_workflow(pipeline)

    # 3. Lấy danh sách item_id của document có eff_status_id là NULL
    null_item_ids = get_null_eff_status_item_ids(pipeline)

    if not null_item_ids:
        logger.info("Không tìm thấy document nào có eff_status_id là NULL để xử lý.")
        return

    # 4. Hỏi xác nhận đồng ý xử lý
    fallback_id = workflow_config.STEP_LOAD_DOCUMENT_LIST.id
    logger.warning(f"Tìm thấy {len(null_item_ids)} documents có eff_status_id là NULL.")
    confirm = input(
        f"Bạn có muốn reset state cho {len(null_item_ids)} documents này về workflow_id={fallback_id} (STEP_LOAD_DOCUMENT_LIST) để xử lý lại không? (y/N): "
    )

    if confirm.strip().lower() in ["y", "yes"]:
        start_time = datetime.now()
        logger.info("Đang thực hiện reset state...")
        log_error_workflow_state(
            pipeline=pipeline,
            error_item_ids=null_item_ids,
            start_time=start_time,
            fallback_workflow_id=fallback_id,
        )
        logger.success("Hoàn thành cập nhật state cho các document NULL.")

        # Thống kê lại sau khi xử lý
        logger.info("--- Thống kê sau khi reset ---")
        get_document_state_count_by_workflow(pipeline)
    else:
        logger.info("Đã hủy bỏ thao tác xử lý lại.")


if __name__ == "__main__":
    main()
