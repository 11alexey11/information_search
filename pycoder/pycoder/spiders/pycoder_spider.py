from pycoder.items import PycoderItem

import scrapy
from urllib.parse import urljoin

class PycoderSpider(scrapy.Spider):
    name = 'pycoder'
    start_urls = ['https://www.kinopoisk.ru/lists/navigator/country-2/?quick_filters=available_online&tab=online']
    visited_urls = []

    def parse(self, response):
        if response.url not in self.visited_urls:
            self.visited_urls.append(response.url)
            SET_SELECTOR='a.selection-film-item-meta__link::attr(href)'
            for post_link in response.css(SET_SELECTOR).extract():
                url = urljoin(response.url, post_link)
                yield response.follow(url, callback=self.parse_film)

            NEXT_PAGE = 'a.paginator__page-relative::attr(href)'
            next_page = response.css(NEXT_PAGE).extract()

            next_page_url = urljoin(response.url, next_page[len(next_page) - 1])

            yield response.follow(next_page_url, callback=self.parse)

    def parse_film(self, response):
        item = PycoderItem()
        title = response.css('h1.styles_root__179-Q > span::text').extract_first()
        body = response.css('p.styles_paragraph__2Otvx::text').extract_first()
        url = response.url
        item['title'] = title
        item['body'] = body
        item['url'] = url
        yield item
