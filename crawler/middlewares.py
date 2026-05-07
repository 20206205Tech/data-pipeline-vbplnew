# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import json
import os
import random

# useful for handling different item types with a single interface
from scrapy import signals
from scrapy.exceptions import NotConfigured


class CrawlerSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    async def process_start(self, start):
        # Called with an async iterator over the spider start() method or the
        # matching method of an earlier spider middleware.
        async for item_or_request in start:
            yield item_or_request

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class CrawlerDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class JsonProxyDownloaderMiddleware:
    def __init__(self, proxy_list):
        self.proxy_list = proxy_list

    @classmethod
    def from_crawler(cls, crawler):
        # Lấy đường dẫn file từ settings, mặc định là 'proxies.json' ở thư mục gốc
        proxy_file_path = crawler.settings.get("PROXY_FILE_PATH", "proxies.json")

        if not os.path.exists(proxy_file_path):
            raise NotConfigured(f"Không tìm thấy file proxy tại: {proxy_file_path}")

        # Đọc file proxies.json
        with open(proxy_file_path, "r", encoding="utf-8") as f:
            raw_proxies = json.load(f)

        # Format lại thành cấu trúc URL chuẩn cho HTTP proxy
        proxy_list = []
        for p in raw_proxies:
            proxy_url = f"http://{p['ip_address']}:{p['port']}"
            proxy_list.append(proxy_url)

        if not proxy_list:
            raise NotConfigured(
                "File proxies.json trống, không có proxy nào được load."
            )

        return cls(proxy_list)

    def process_request(self, request, spider):
        # Nếu request đã có proxy (ví dụ set cứng trong spider), thì bỏ qua
        if "proxy" in request.meta:
            return None

        # Chọn ngẫu nhiên một proxy từ danh sách
        proxy = random.choice(self.proxy_list)
        request.meta["proxy"] = proxy

        # In log để dễ debug
        spider.logger.debug(f"Đang sử dụng proxy: {proxy} cho URL: {request.url}")
