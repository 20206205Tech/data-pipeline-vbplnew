import scrapy
from loguru import logger

import env


def make_vbpl_page_request(spider_instance, page, row_per_page=None):
    if row_per_page is None:
        row_per_page = 10 if env.CRAWL_DATA_ENV_DEV else 50

    target_url = "https://vbpl-bientap-gateway.moj.gov.vn/api/qtdc/public/doc/all"
    target_payload = {"pageSize": row_per_page, "pageNumber": page}

    proxy_url = "https://20206205.work.gd/"
    proxy_payload = {"url": target_url, "method": "POST", "json": target_payload}

    logger.debug(f"Đang tạo request cho trang {page}: {target_url} (qua proxy)")

    return scrapy.http.JsonRequest(
        url=proxy_url,
        method="POST",
        data=proxy_payload,
        callback=spider_instance.parse,
        meta={"current_page": page},
    )
