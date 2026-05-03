import math
from datetime import datetime

import psycopg2
import scrapy
from loguru import logger
from psycopg2 import Error
from scrapy.utils.response import open_in_browser

import env
from utils.request_helper import make_vbpl_page_request


class DocumentListSpider(scrapy.Spider):
    name = "document_list"
    allowed_domains = ["vbpl-bientap-gateway.moj.gov.vn", "20206205.work.gd"]

    def __init__(self, *args, **kwargs):
        super(DocumentListSpider, self).__init__(*args, **kwargs)

        self.row_per_page = 2 if env.CRAWL_DATA_ENV_DEV else 50
        self.max_pages = 1
        self.determine_crawl_limit()

    def determine_crawl_limit(self):
        connection = None
        try:
            connection = psycopg2.connect(env.DATABASE_URL)
            cursor = connection.cursor()

            sql_query = """
                SELECT total_count
                FROM "public"."document_total"
                ORDER BY update_at DESC
                LIMIT 2;
            """

            cursor.execute(sql_query)
            records = cursor.fetchall()

            default_total = 5 if env.CRAWL_DATA_ENV_DEV else 1000
            diff = default_total

            if len(records) >= 2:
                latest = records[0][0]
                previous = records[1][0]

                if latest != previous:
                    diff = abs(latest - previous)
                    logger.info(
                        f"Phát hiện thay đổi: {previous} -> {latest}. Diff = {diff}"
                    )
                else:
                    logger.info(
                        f"Không có thay đổi tổng số. Sử dụng diff mặc định: {default_total}"
                    )

            elif len(records) == 1:
                latest = records[0][0]
                # Nếu chỉ có 1 bản ghi, lấy abs giữa bản ghi đó và 0 (coi như chưa có dữ liệu cũ)
                # Nếu bạn muốn so sánh với số mặc định, thay 0 bằng default_total: abs(latest - default_total)
                diff = abs(latest - 0)
                logger.info(
                    f"Chỉ có 1 bản ghi tổng số (latest = {latest}). Diff = {diff}"
                )

            else:
                logger.info(
                    f"Không có bản ghi nào. Sử dụng diff mặc định: {default_total}"
                )

            self.max_pages = math.ceil(diff / self.row_per_page) + 1

            logger.info(
                f"Kết quả tính toán: max_pages = {self.max_pages} (row_per_page = {self.row_per_page})"
            )

        except (Exception, Error) as error:
            logger.error(
                f"Lỗi khi truy xuất database: {error}. Sử dụng cấu hình dự phòng."
            )
            self.max_pages = 2 if env.CRAWL_DATA_ENV_DEV else 10
        finally:
            if connection:
                cursor.close()
                connection.close()

    def start_requests(self):
        yield make_vbpl_page_request(self, page=1, row_per_page=self.row_per_page)

    def parse(self, response):
        if env.CRAWL_DATA_OPEN_IN_BROWSER:
            open_in_browser(response)

        current_page = response.meta.get("current_page", 1)

        try:
            data = response.json()
            items = data.get("data", {}).get("items", [])
        except Exception as e:
            logger.error(f"Lỗi khi parse JSON tại trang {current_page}: {e}")
            return

        if not items:
            logger.warning(
                f"Không tìm thấy dữ liệu tại trang {current_page}. Dừng spider."
            )
            return

        for item in items:
            # Lưu item_id là chuỗi UUID từ API mới
            item_data = item.copy()
            item_data["item_id"] = item.get("id")
            item_data["update_at"] = datetime.now().isoformat()

            yield item_data

        if current_page < self.max_pages:
            yield make_vbpl_page_request(
                self, page=current_page + 1, row_per_page=self.row_per_page
            )
