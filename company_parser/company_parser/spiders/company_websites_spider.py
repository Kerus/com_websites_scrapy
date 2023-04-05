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
            'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
            'scrapy_playwright.browser.BrowserMiddleware': 543
        },
        'TWISTED_REACTOR': "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        'PLAYWRIGHT_LAUNCH_OPTIONS' : {
            'headless': True,
        },
        'PLAYWRIGHT_BROWSER_TYPE': 'firefox'
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

            soup = BeautifulSoup(response.body, "html.parser")
            div = soup.find_all("div", id="links")
            if len(div) > 0:
                results = div[0].find_all("a")
                for result in results:
                    href = result.get("href")
                    if href and href.startswith("http") and all(stop not in href for stop in stop_words):
                        website = href
                        website = urllib.parse.unquote(website)
                        parsed = urllib.parse.urlparse(website)
                        website = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
        item = CompanyItem(name=name, website=website)
        return item
        
