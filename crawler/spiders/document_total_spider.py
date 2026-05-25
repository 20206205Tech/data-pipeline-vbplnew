from datetime import datetime

import scrapy
from loguru import logger
from scrapy.utils.response import open_in_browser

import env
from utils.request_helper import make_vbpl_page_request


class DocumentTotalSpider(scrapy.Spider):
    name = "document_total"

    allowed_domains = ["vbpl-bientap-gateway.moj.gov.vn"]
    allowed_domains.extend(env.PROXY_GATEWAYS)

    async def start(self):
        yield make_vbpl_page_request(
            self, page=1, row_per_page=1, sortDirection=None, sortBy=None, isNew=None
        )

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
