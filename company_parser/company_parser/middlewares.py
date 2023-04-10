# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from scrapy.exceptions import NotConfigured

from itertools import cycle

class ProxyPlayWrightMiddleware:
    def __init__(self, proxies):
        self.proxy_requests = {}
        for proxy in proxies:
            self.proxy_requests[proxy] = 0

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getlist('PROXY_LIST'):
            raise NotConfigured('No proxies provided')
        proxies = crawler.settings.getlist('PROXY_LIST')
        return cls(proxies)

    def process_request(self, request, spider):
        proxy, _ = min(self.proxy_requests.items(), key=lambda x: x[1])
        request.meta['playwright'] = True
        
        request.meta['playwright_context'] = str(hash(request.meta.get('name', 'noname')))
        request.meta['playwright_context_kwargs'] = {
            "java_script_enabled": True,
            "ignore_https_errors": True,
            "proxy": {
                "server": proxy,
            },
        }
        
        request.meta['proxy_info'] = proxy
        spider.logger.debug(f'Using proxy {proxy}, which has been used {self.proxy_requests[proxy]} times so far.')
        self.proxy_requests[proxy] += 1
        
        
    def process_exception(self, request, exception, spider):
        proxy = request.meta.get('proxy_info')
        if proxy and self.proxy_requests[proxy] > 1:
            self.proxy_requests[proxy] -= 1
            spider.logger.debug(f'Removing proxy {proxy} from request {request}, which has been used {self.proxy_requests[proxy]} times so far.')
            self.proxy_requests.pop(proxy, None)
            if len(self.proxy_requests) == 0:
                raise NotConfigured('All proxies are unusable')


class CompanyParserSpiderMiddleware:
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

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


class CompanyParserDownloaderMiddleware:
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
