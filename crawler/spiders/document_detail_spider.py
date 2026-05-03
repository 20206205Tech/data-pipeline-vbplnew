import os
from datetime import datetime

import psycopg2
import scrapy
from loguru import logger
from scrapy.exceptions import CloseSpider
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy.utils.response import open_in_browser
from twisted.internet.error import TCPTimedOutError, TimeoutError

import env
from output_document_detail import PATH_FOLDER_OUTPUT
from utils.workflow_helper import fetch_and_lock_pending_tasks


class DocumentDetailSpider(scrapy.Spider):
    name = "document_detail"
    allowed_domains = ["vbpl-bientap-gateway.moj.gov.vn", "20206205.work.gd"]

    def _get_connection(self):
        return psycopg2.connect(env.DATABASE_URL)

    def start_requests(self):
        pending_item_ids = []
        conn = None
        try:
            conn = self._get_connection()
            with conn:
                pending_item_ids = fetch_and_lock_pending_tasks(
                    conn=conn,
                    step_code="step_crawl_document_detail",
                    limit=2 if env.CRAWL_DATA_ENV_DEV else 50,
                    # limit=2 if env.CRAWL_DATA_ENV_DEV else 50 * 15 * 4 ,
                )
        except Exception as e:
            logger.error(f"Lỗi khi lấy logic database từ PostgreSQL: {e}")
            return
        finally:
            if conn:
                conn.close()

        if not pending_item_ids:
            logger.info("🎉 Không còn bản ghi nào cần crawl.")
            return

        for item_id in pending_item_ids:
            target_url = (
                f"https://vbpl-bientap-gateway.moj.gov.vn/api/qtdc/public/doc/{item_id}"
            )
            proxy_url = "https://20206205.work.gd/"
            proxy_payload = {"url": target_url, "method": "GET"}

            yield scrapy.http.JsonRequest(
                url=proxy_url,
                method="POST",
                data=proxy_payload,
                callback=self.parse_detail,
                errback=self.handle_error,
                meta={"item_id": item_id},
            )

    def handle_error(self, failure):
        item_id = failure.request.meta.get("item_id")
        logger.error(f"❌ Lỗi Request tại item {item_id}: {repr(failure)}")

        if failure.check(TimeoutError, TCPTimedOutError):
            logger.error(
                "🛑 Website không phản hồi sau 2 phút! Đang hủy bỏ tất cả các URL còn lại..."
            )
            raise CloseSpider("server_timeout")

        if failure.check(HttpError):
            response = failure.value.response
            if response.status >= 500:
                logger.error(
                    f"🛑 Server trả về mã lỗi {response.status}! Đang hủy bỏ tất cả các URL còn lại..."
                )
                raise CloseSpider(f"server_error_{response.status}")

    def parse_detail(self, response):
        if env.CRAWL_DATA_OPEN_IN_BROWSER:
            open_in_browser(response)

        item_id = response.meta.get("item_id")

        if response.status == 200:
            try:
                data = response.json()
            except Exception as e:
                logger.error(f"Lỗi khi parse JSON item {item_id}: {e}")
                logger.error(f"Response URL: {response.url}")
                logger.error(f"Response Headers: {response.headers}")
                logger.error(f"Response Text (first 500 chars): {response.text[:500]}")
                return

            if not data.get("success") or "data" not in data:
                logger.warning(
                    f"⚠️ API trả về không thành công hoặc không có data cho item: {item_id}"
                )
                logger.debug(data)
                return

            item_data = data["data"]

            # Trích xuất HTML content nếu có
            doc_content = item_data.get("documentContent")
            if (
                doc_content
                and isinstance(doc_content, dict)
                and doc_content.get("content")
            ):
                file_path = os.path.join(PATH_FOLDER_OUTPUT, f"{item_id}.html")
                os.makedirs(PATH_FOLDER_OUTPUT, exist_ok=True)

                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(doc_content["content"])

                logger.info(f"✅ Đã lưu HTML detail: {item_id}")

                # Xóa phần nội dung HTML nặng ra khỏi item_data để tránh log/jsonl bị phình to
                del item_data["documentContent"]["content"]
            else:
                logger.info(
                    f"⚠️ Item {item_id} không có nội dung HTML (hasContent=False)."
                )

            # Gán thêm metadata hệ thống
            item_data["update_at"] = datetime.now().isoformat()
            item_data["item_id"] = item_id

            yield item_data
        else:
            logger.warning(
                f"❌ Crawl thất bại (Status {response.status}) cho item: {item_id}"
            )
