from scrapy import Spider
from pep_parse.items import PepParseItem


class PepSpider(Spider):
    name = 'pep'
    allowed_domains = ['peps.python.org']
    start_urls = ['https://peps.python.org/']
    custom_settings = {'FEEDS': None}

    def parse(self, response):
        for pep_link in response.css('tbody tr a[href^="pep-"]'):
            yield response.follow(pep_link, callback=self.parse_pep)

    def parse_pep(self, response):
        data = {
            'number': response.css('h1.page-title::text').get().split()[1],
            'name': response.css(
                'h1.page-title::text'
            ).get().split(' – ')[1],
            'status': response.css('abbr::text').get(),
        }
        yield PepParseItem(data)
