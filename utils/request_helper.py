import scrapy
from loguru import logger

import env


def make_vbpl_page_request(spider_instance, page, row_per_page=None):
    if row_per_page is None:
        row_per_page = 10 if env.CRAWL_DATA_ENV_DEV else 50

    url = "https://vbpl-bientap-gateway.moj.gov.vn/api/qtdc/public/doc/all"
    payload = {"pageSize": row_per_page, "pageNumber": page}

    logger.debug(f"Đang tạo request cho trang {page}: {url}")

    return scrapy.http.JsonRequest(
        url=url,
        data=payload,
        callback=spider_instance.parse,
        meta={"current_page": page},
    )
