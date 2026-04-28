import os
from datetime import datetime

import dlt
import psycopg2
from loguru import logger

import env
import workflow_config
from output_document_detail import PATH_FILE_OUTPUT, PATH_FOLDER_OUTPUT
from utils.config_by_path import ConfigByPath
from utils.google_drive import get_drive_file_md5, get_drive_service, upload_to_drive
from utils.hash_helper import calculate_file_md5, get_existing_drive_ids_from_db
from utils.jsonl_helper import yield_jsonl_records
from utils.workflow_helper import (
    document_state_resource,
    log_error_workflow_state,
    log_workflow_state,
)

config_by_path = ConfigByPath(__file__)


def process_drive_upload(
    records,
    conn,
    drive_service,
    success_item_ids,
    error_item_ids,
    fast_forward_item_ids,
):
    success_records = []
    all_item_ids = [r.get("item_id") for r in records if r.get("item_id")]

    if not all_item_ids:
        return []

    # Lấy drive_id cũ từ bảng documents
    dict_drive_ids = get_existing_drive_ids_from_db(
        conn, "documents", all_item_ids, "drive_id"
    )

    for record in records:
        item_id = record.get("item_id")
        if not item_id:
            continue

        html_path = os.path.join(PATH_FOLDER_OUTPUT, f"{item_id}.html")

        if not os.path.exists(html_path):
            logger.info(
                f"⚠️ File HTML không tồn tại cho item_id {item_id} (có thể API không có nội dung). Bỏ qua upload và đưa thẳng thành ID {workflow_config.STEP_RAG_EMBEDDING.id}."
            )
            record["drive_id"] = None
            success_records.append(record)
            fast_forward_item_ids.append(item_id)
            continue

        local_md5 = calculate_file_md5(html_path)
        if not local_md5:
            error_item_ids.append(item_id)
            continue

        drive_id = dict_drive_ids.get(str(item_id))

        if drive_id:
            drive_md5 = get_drive_file_md5(drive_service, drive_id)
            if drive_md5 == local_md5:
                logger.info(f"File không đổi trên Drive, bỏ qua upload: {item_id}")
                record["drive_id"] = drive_id
                success_records.append(record)
                success_item_ids.append(item_id)
                continue

        new_drive_id = upload_to_drive(
            drive_service,
            html_path,
            config_by_path.GOOGLE_DRIVE_FOLDER_ID_DATA_PIPELINE_VBPL,
        )

        if not new_drive_id:
            logger.error(f"Upload thất bại, cần thu thập lại item_id: {item_id}")
            error_item_ids.append(item_id)
            continue

        logger.info(f"Đã upload thành công HTML cho item_id: {item_id}")
        record["drive_id"] = new_drive_id
        success_records.append(record)
        success_item_ids.append(item_id)

    return success_records


@dlt.resource(
    name="documents",
    write_disposition="merge",
    primary_key="item_id",
    columns={
        "has_original_pdf": {"data_type": "bool"},
        "lang": {"data_type": "text"},
        "review_status": {"data_type": "text"},
    },
)
def get_document_details(records):
    for r in records:
        yield {
            "item_id": r.get("item_id"),
            "drive_id": r.get("drive_id"),
            "view_count": r.get("viewCount"),
            "document_content_file_name": r.get("documentContentFileName"),
            "document_content_file_doc_name": r.get("documentContentFileDocName"),
            "is_old": r.get("isOld"),
            "is_effect_all_document": r.get("isEffectAllDocument"),
            "review_status": r.get("reviewStatus"),
            "has_content": r.get("hasContent"),
            "has_original_pdf": r.get("hasOriginalPdf"),
            "has_ai_processed": r.get("hasAIProcessed"),
            "agency_name": r.get("agencyName"),
            "lang": r.get("lang"),
            "status": r.get("status"),
        }


@dlt.resource(name="document_issues", write_disposition="merge", primary_key="id")
def get_document_issues(records):
    for r in records:
        doc_id = r.get("item_id")
        for i in r.get("documentIssues") or []:
            if i.get("id"):
                yield {
                    "id": i.get("id"),
                    "document_id": doc_id,
                    "agency_id": i.get("agencyId"),
                    "agency_name": i.get("agencyName"),
                    "person_id": i.get("personId"),
                    "person_name": i.get("personName"),
                    "job_title_code": i.get("jobTitleCode"),
                    "job_title_name": i.get("jobTitleName"),
                    "order_index": i.get("orderIndex"),
                }


@dlt.resource(name="document_references", write_disposition="merge", primary_key="id")
def get_document_references(records):
    for r in records:
        doc_id = r.get("item_id")
        for ref in r.get("references") or []:
            if ref.get("id"):
                td = ref.get("targetDocument") or {}
                yield {
                    "id": ref.get("id"),
                    "document_id": doc_id,
                    "target_document_id": td.get("id"),
                    "target_document_type": td.get("docType"),
                    "target_document_num": td.get("docNum"),
                    "target_document_title": td.get("title"),
                    "target_issue_date": td.get("issueDate"),
                    "target_eff_from": td.get("effFrom"),
                    "target_status": td.get("status"),
                    "reference_type": ref.get("referenceType"),
                }


def main():
    pipeline = dlt.pipeline(
        destination="postgres",
        dataset_name="public",
        pipeline_name=config_by_path.NAME,
    )

    records = list(yield_jsonl_records(PATH_FILE_OUTPUT))
    logger.info(f"Tổng số dòng cần xử lý từ file JSONL: {len(records)}")

    if not records:
        logger.info("🎉 Không có dữ liệu để xử lý.")
        return

    success_item_ids = []
    error_item_ids = []
    fast_forward_item_ids = []
    start_time = datetime.now()

    conn = None
    try:
        conn = psycopg2.connect(env.DATABASE_URL)
        drive_service = get_drive_service()

        # Tiền xử lý Upload Drive
        success_records = process_drive_upload(
            records,
            conn,
            drive_service,
            success_item_ids,
            error_item_ids,
            fast_forward_item_ids,
        )

        if success_records:
            logger.info(
                f"Đang load {len(success_records)} records vào Database qua dlt..."
            )
            load_info = pipeline.run(
                [
                    get_document_details(success_records),
                    get_document_issues(success_records),
                    get_document_references(success_records),
                ]
            )
            logger.info(f"Kết quả dlt pipeline run: {load_info}")

    except Exception as e:
        logger.error(f"Lỗi khi xử lý pipeline: {e}")
    finally:
        if conn:
            conn.close()

    # Ghi log workflow
    if success_item_ids:
        log_workflow_state(
            pipeline=pipeline,
            item_ids=success_item_ids,
            start_time=start_time,
            end_time=datetime.now(),
        )
        logger.info(f"Đã xử lý thành công {len(success_item_ids)} items.")

    if fast_forward_item_ids:
        pipeline.run(
            document_state_resource(
                workflow_id=workflow_config.STEP_RAG_EMBEDDING.id,
                item_ids=fast_forward_item_ids,
                start_time=start_time,
                end_time=datetime.now(),
            )
        )
        logger.info(
            f"⏩ Đã fast-forward {len(fast_forward_item_ids)} items không có nội dung lên bước cuối."
        )

    if error_item_ids:
        logger.error(f"Có {len(error_item_ids)} items gặp lỗi và cần thu thập lại.")
        logger.warning(f"Danh sách lỗi: {error_item_ids}")
        log_error_workflow_state(pipeline, error_item_ids, start_time)


if __name__ == "__main__":
    main()
