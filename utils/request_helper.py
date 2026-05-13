import random

import scrapy
from loguru import logger

import env


def make_vbpl_page_request(spider_instance, page, row_per_page=None, **kwargs):
    if row_per_page is None:
        row_per_page = 10 if env.CRAWL_DATA_ENV_DEV else 50

    target_url = "https://vbpl-bientap-gateway.moj.gov.vn/api/qtdc/public/doc/all"

    # Mặc định các tham số cho list spider
    target_payload = {
        "sortDirection": "desc",
        "sortBy": "issueDate",
        "pageSize": row_per_page,
        "pageNumber": page,
        "isNew": True,
    }

    # Ghi đè hoặc thêm bớt tham số từ kwargs
    target_payload.update(kwargs)

    # Loại bỏ các tham số có giá trị là None (dùng để xóa tham số mặc định)
    target_payload = {k: v for k, v in target_payload.items() if v is not None}

    proxy_url = f"https://{random.choice(env.PROXY_GATEWAYS)}/"
    proxy_payload = {"url": target_url, "method": "POST", "json": target_payload}

    logger.debug(f"Đang tạo request cho trang {page}: {target_url} (qua proxy)")

    return scrapy.http.JsonRequest(
        url=proxy_url,
        method="POST",
        data=proxy_payload,
        callback=spider_instance.parse,
        meta={"current_page": page},
    )