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


def chunked_iterable(iterable, size):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def process_drive_upload(
    records,
    conn,
    drive_service,
    success_item_ids,
    error_item_ids,
    fast_forward_item_ids,
    fast_forward_to_parent_item_ids,
):
    success_records = []
    # Lấy item_id, fallback về id nếu spider truyền trực tiếp raw data
    all_item_ids = [
        r.get("item_id") or str(r.get("id"))
        for r in records
        if r.get("item_id") or r.get("id")
    ]

    if not all_item_ids:
        return []

    # Lấy drive_id cũ từ bảng documents
    dict_drive_ids = get_existing_drive_ids_from_db(
        conn, "documents", all_item_ids, "drive_id"
    )

    for record in records:
        item_id = record.get("item_id") or str(record.get("id"))
        if not item_id:
            continue

        record["item_id"] = item_id  # Chuẩn hóa lại key để dùng cho dlt resource

        html_path = os.path.join(PATH_FOLDER_OUTPUT, f"{item_id}.html")

        if not os.path.exists(html_path):
            logger.info(
                f"⚠️ File HTML không tồn tại cho item_id {item_id} (có thể API không có nội dung). Bỏ qua upload và đưa thẳng thành ID {workflow_config.STEP_RAG_EMBEDDING.id}."
            )
            record["drive_id"] = None
            success_records.append(record)
            if record.get("status") == "404":
                fast_forward_to_parent_item_ids.append(item_id)
            else:
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
            config_by_path.GOOGLE_DRIVE_FOLDER_ID,
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


# ==========================================
# CÁC DLT RESOURCES BÓC TÁCH DIMENSIONS (Kế thừa từ List)
# ==========================================


@dlt.resource(name="dim_doc_type", write_disposition="merge", primary_key="id")
def get_dim_doc_types(records):
    seen = set()
    for r in records:
        doc = r.get("docType") or {}
        id_val = doc.get("id")
        if id_val and id_val not in seen:
            seen.add(id_val)
            yield {"id": id_val, "code": doc.get("code"), "name": doc.get("name")}


@dlt.resource(name="dim_eff_status", write_disposition="merge", primary_key="id")
def get_dim_eff_statuses(records):
    seen = set()
    for r in records:
        eff = r.get("effStatus") or {}
        id_val = eff.get("id")
        if id_val and id_val not in seen:
            seen.add(id_val)
            yield {"id": id_val, "code": eff.get("code"), "name": eff.get("name")}


@dlt.resource(name="dim_major", write_disposition="merge", primary_key="code")
def get_dim_majors(records):
    seen = set()
    for r in records:
        for m in r.get("documentMajors") or []:
            mt = m.get("majorType") or {}
            id_val = m.get("id")
            if id_val and id_val not in seen:
                seen.add(id_val)
                yield {
                    "id": id_val,
                    "code": mt.get("code"),
                    "name": mt.get("name"),
                    "short_name": mt.get("shortName"),
                }


@dlt.resource(
    name="document_majors",
    write_disposition="merge",
    primary_key=["document_id", "major_id"],
)
def get_document_majors(records):
    seen = set()
    for r in records:
        doc_id = r.get("item_id")
        for m in r.get("documentMajors") or []:
            major_id = m.get("id")
            key = (doc_id, major_id)
            if doc_id and major_id and key not in seen:
                seen.add(key)
                yield {"document_id": doc_id, "major_id": major_id}


@dlt.resource(
    name="document_related_files", write_disposition="merge", primary_key="id"
)
def get_document_related_files(records):
    seen = set()
    for r in records:
        doc_id = r.get("item_id")
        for f in r.get("documentRelatedList") or []:
            file_id = f.get("id")
            if file_id and file_id not in seen:
                seen.add(file_id)
                yield {
                    "id": file_id,
                    "document_id": doc_id,
                    "file_name": f.get("fileName"),
                    "related_type": f.get("relatedType"),
                    "file_title": f.get("fileTitle"),
                    "file_order": f.get("fileOrder"),
                }


# ==========================================
# CÁC DLT RESOURCES CHI TIẾT (Detail)
# ==========================================


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
            # Bổ sung các trường từ API Detail
            "title": r.get("title"),
            "doc_num": r.get("docNum"),
            "doc_abs": r.get("docAbs"),
            "doc_type_id": (r.get("docType") or {}).get("id"),
            "eff_status_id": (r.get("effStatus") or {}).get("id"),
            "issue_date": r.get("issueDate"),
            "eff_from": r.get("effFrom"),
            "eff_to": r.get("effTo"),
            "public_date": r.get("publicDate"),
            "updated_date": r.get("updatedDate"),
            "is_new": r.get("isNew"),
            "is_lw": r.get("isLw"),
            "source_document_id": r.get("sourceDocumentId"),
            # Các trường gốc của Detail
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

    BATCH_SIZE = (
        100  # Nên đặt nhỏ hơn List vì Detail có kèm theo upload lên Google Drive
    )
    success_item_ids = []
    error_item_ids = []
    fast_forward_item_ids = []
    fast_forward_to_parent_item_ids = []
    total_loaded = 0
    start_time = datetime.now()

    logger.info(f"Bắt đầu load dữ liệu với BATCH_SIZE = {BATCH_SIZE}...")
    drive_service = get_drive_service()

    for batch_idx, batch in enumerate(
        chunked_iterable(yield_jsonl_records(PATH_FILE_OUTPUT), BATCH_SIZE)
    ):
        conn = None
        try:
            conn = psycopg2.connect(env.DATABASE_URL)

            # Tiền xử lý Upload Drive cho từng Batch
            success_records = process_drive_upload(
                batch,
                conn,
                drive_service,
                success_item_ids,
                error_item_ids,
                fast_forward_item_ids,
                fast_forward_to_parent_item_ids,
            )

            if success_records:
                logger.info(
                    f"Đang chuẩn bị load batch {batch_idx + 1} ({len(success_records)} records) vào Database..."
                )
                load_info = pipeline.run(
                    [
                        get_dim_doc_types(success_records),
                        get_dim_eff_statuses(success_records),
                        get_dim_majors(success_records),
                        get_document_details(success_records),
                        get_document_majors(success_records),
                        get_document_related_files(success_records),
                        get_document_issues(success_records),
                        get_document_references(success_records),
                    ]
                )
                logger.info(f"Hoàn thành batch {batch_idx + 1}.")
                total_loaded += len(success_records)

        except Exception as e:
            logger.error(f"Lỗi khi xử lý pipeline ở batch {batch_idx + 1}: {e}")
        finally:
            if conn:
                conn.close()

    if total_loaded == 0 and not error_item_ids:
        logger.info("🎉 Không có dữ liệu để xử lý.")
        return
    else:
        logger.info(f"Hoàn thành load tổng cộng {total_loaded} records vào Database.")

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

    if fast_forward_to_parent_item_ids:
        pipeline.run(
            document_state_resource(
                workflow_id=workflow_config.STEP_RAG_CONTEXT.id,
                item_ids=fast_forward_to_parent_item_ids,
                start_time=start_time,
                end_time=datetime.now(),
            )
        )
        logger.info(
            f"⏩ Đã fast-forward {len(fast_forward_to_parent_item_ids)} items 404 lên parent ID {workflow_config.STEP_RAG_CONTEXT.id} để bước RAG_EMBEDDING thực hiện xóa vector."
        )

    if error_item_ids:
        logger.error(f"Có {len(error_item_ids)} items gặp lỗi và cần thu thập lại.")
        logger.warning(f"Danh sách lỗi: {error_item_ids}")
        log_error_workflow_state(pipeline, error_item_ids, start_time)


if __name__ == "__main__":
    main()
