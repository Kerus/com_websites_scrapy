import scrapy
import pandas as pd
import os
from bs4 import BeautifulSoup
import urllib.parse
from scrapy.utils.project import get_project_settings
from company_parser.items import CompanyItem

class CompanyWebSiteSpider(scrapy.Spider):
    name = "company_websites_spider"

    custom_settings = {
        "DOWNLOADER_MIDDLEWARES": {
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400
        },
        'REDIRECT_ENABLED': False,
        'TWISTED_REACTOR': "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        }
    }
    
    def start_requests(self):
        fname = os.path.join(os.getcwd(), 'full_sample.xlsx')
        companies_data = pd.read_excel(fname)
        companies_data = companies_data.head(1)
        keywords = ["official website", "homepage"]
        for i, company in companies_data.iterrows():
            company_name = company["Account Name"]
            query = f"{'+'.join(company_name.split())}+" + "+".join(keywords)
            url = f"https://duckduckgo.com/?q={query}"
            yield scrapy.Request(url=url, callback=self.parse, meta={'name': company_name, 'playwright': True})

    def parse(self, response):

        name = response.meta['name']
        website = None
        stop_words = ["wikipedia", "linkedin", "facebook", "crunchbase", "https://craft.co"]

        if response.status == 200:
            links = response.xpath('//div[@id="links"]//h2/a[contains(@href,"http")]/@href').getall()
            stop_words_filter = filter(lambda x: all(stop not in x for stop in stop_words), links)
            unquote_filter = filter(lambda x: urllib.parse.unquote(x), stop_words_filter)
            path_filter = filter(lambda x: len(urllib.parse.urlparse(x).path.split('/')) < 3, unquote_filter)
            links = list(path_filter)
            if len(links) > 0:
                website = links[0]
        item = CompanyItem(name=name, website=website)
        return item
        
