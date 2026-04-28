from datetime import datetime

import scrapy
from loguru import logger
from scrapy.http import JsonRequest
from scrapy.utils.response import open_in_browser

import env


class DocumentTotalSpider(scrapy.Spider):
    name = "document_total"
    allowed_domains = ["vbpl-bientap-gateway.moj.gov.vn"]

    def start_requests(self):
        target_url = "https://vbpl-bientap-gateway.moj.gov.vn/api/qtdc/public/doc/all"
        target_payload = {"pageSize": 1, "pageNumber": 1}
        proxy_url = "https://20206205.work.gd/"
        proxy_payload = {"url": target_url, "method": "POST", "json": target_payload}
        yield JsonRequest(url=proxy_url, data=proxy_payload, callback=self.parse)

    def parse(self, response):
        if env.CRAWL_DATA_OPEN_IN_BROWSER:
            open_in_browser(response)

        try:
            data = response.json()
            web_total = data.get("data", {}).get("total")

            if web_total is not None:
                logger.info(f"Tổng số văn bản hiện tại trên API: {web_total}")

                yield {
                    "update_at": datetime.now().isoformat(),
                    "total_count": web_total,
                }
            else:
                logger.warning(
                    f"Không tìm thấy thông tin tổng số văn bản. Dữ liệu: {data}"
                )
        except Exception as e:
            logger.error(f"Lỗi khi đọc JSON. Body: {response.body[:200]}")
            logger.error(f"Exception: {e}")
