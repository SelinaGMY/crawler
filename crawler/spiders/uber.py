import json

import scrapy
from scrapy.http.request import Request


class UberSpider(scrapy.Spider):
    name = 'uber'
    allowed_domains = ['uber.com']
    start_urls = ['http://uber.com/']
    custom_settings = {
        'DOWNLOAD_DELAY': 3,       #间隔3s请求一次
        'DEFAULT_REQUEST_HEADERS': {
            'authority': 'www.uber.com',
            'content-type': 'application/json',
            'origin': 'https://www.uber.com',
            'x-csrf-token': 'x',
            'referer': 'https://www.uber.com/us/en/careers/list/',
            'accept-language': 'zh-CN,zh;q=0.9',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.80 Safari/537.36',
            'accept': '*/*',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'cookie': '_ua={"session_id":"51653723-e26e-4f8e-9299-c5715471cd01","session_time_ms":1603876544684}; marketing_vistor_id=672268b0-9043-49fb-abde-23e2a03a284a'
        }
    }

    def start_requests(self):
        body = {
            "params": {"allLocation": [], "Sector": [], "Subteam": [], "programAndPlatform": [],
                       "lineOfBusinessName": []},
            "limit": 10,
            "page": 0
        }

        yield Request(
            url="https://www.uber.com/api/loadSearchJobsResults?localeCode=en",
            method='POST',
            body=json.dumps(body),
            callback=self.parse_data,
            meta={"page": 1, "body": body},
        )

    def parse_data(self, response):
        resp = json.loads(response.body)
        items = resp.get('data', {}).get('results', [])
        for item in items:
            yield item

        total_num = resp.get('data', {}).get('totalResults', {}).get('low', 0)
        if total_num > 0 and response.meta['page'] * 10 < total_num:
            body = response.meta['body']
            body['page'] = response.meta['page']
            yield Request(
                url="https://www.uber.com/api/loadSearchJobsResults?localeCode=en",
                method='POST',
                body=json.dumps(body),
                callback=self.parse_data,
                meta={'page': response.meta['page'] + 1, 'body': response.meta['body']},
            )
