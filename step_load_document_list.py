from datetime import datetime

import dlt
from loguru import logger

from output_document_list import PATH_FILE_OUTPUT
from utils.config_by_path import ConfigByPath
from utils.jsonl_helper import yield_jsonl_records
from utils.workflow_helper import log_workflow_state

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


@dlt.resource(name="dim_major", write_disposition="merge", primary_key="id")
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


@dlt.resource(name="documents", write_disposition="merge", primary_key="item_id")
def get_documents(records):
    for r in records:
        yield {
            "item_id": r.get("item_id"),
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


def main():
    pipeline = dlt.pipeline(
        destination="postgres",
        dataset_name="public",
        pipeline_name=config_by_path.NAME,
    )

    BATCH_SIZE = 500
    all_item_ids = []
    total_loaded = 0

    logger.info(f"Bắt đầu load dữ liệu với BATCH_SIZE = {BATCH_SIZE}...")

    for batch_idx, batch in enumerate(
        chunked_iterable(yield_jsonl_records(PATH_FILE_OUTPUT), BATCH_SIZE)
    ):
        item_ids = [r.get("item_id") for r in batch if r.get("item_id")]
        if not item_ids:
            continue

        all_item_ids.extend(item_ids)

        # Load data vào database cho batch hiện tại
        logger.info(
            f"Đang chuẩn bị load batch {batch_idx + 1} ({len(batch)} records) vào Database..."
        )
        load_info = pipeline.run(
            [
                get_dim_doc_types(batch),
                get_dim_eff_statuses(batch),
                get_dim_majors(batch),
                get_documents(batch),
                get_document_majors(batch),
                get_document_related_files(batch),
            ]
        )
        logger.info(f"Hoàn thành batch {batch_idx + 1}.")
        total_loaded += len(batch)

    if not all_item_ids:
        logger.warning("Không có item_id nào được thu thập. Bỏ qua ghi log.")
        return

    logger.info(f"Hoàn thành load tổng cộng {total_loaded} records vào Database.")

    # Ghi log workflow
    if all_item_ids:
        now = datetime.now()
        log_workflow_state(
            pipeline=pipeline, item_ids=all_item_ids, start_time=now, end_time=now
        )


if __name__ == "__main__":
    main()
