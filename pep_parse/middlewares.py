from scrapy import signals


class PepParse:
    @classmethod
    def from_crawler(cls, crawler):
        instance = cls()
        crawler.signals.connect(
            instance.spider_opened, signal=signals.spider_opened
        )
        return instance

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class PepParseSpiderMiddleware(PepParse):
    def process_spider_input(self, response, spider):
        return None

    def process_spider_output(self, response, result, spider):
        for item in result:
            yield item

    def process_spider_exception(self, response, exception, spider):
        pass

    def process_start_requests(self, start_requests, spider):
        for request in start_requests:
            yield request


class PepParseDownloaderMiddleware(PepParse):
    def process_request(self, request, spider):
        return None

    def process_response(self, request, response, spider):
        return response

    def process_exception(self, request, exception, spider):
        pass
