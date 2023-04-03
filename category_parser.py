import  requests
import logging
from datetime import datetime
import collections
import bs4
import re
import  time
from datetime import date
import json
import  lxml
import  os

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('wb')

class Client:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Accept-Language':'ru'
        }
        self.result = {}
        self.url = 'https://www.perekrestok.ru/cat'

    def load_page(self, url):
        while True:
            try:
                res = self.session.get(url=url)
                res.raise_for_status()
                break
            except Exception as e:
                print('Exception ',e,'. Trying again...')
        return res.text


    def parse_page(self, text: str):
        soup = bs4.BeautifulSoup(text,'lxml')
        container = soup.select('div.category-filter-item')
        for block in container:
            self.parse_block(block=block)
        #self.result['start_time'] = round(time.time())
        #self.result['operdate'] = date.today().strftime('%Y%m%d')



    def parse_block(self, block):
        #logger.info(block)
        #logger.info('=' * 100)
        big_category_name = block.select_one('a').text
        little_category_block = block.select('div.category-filter-item__content')
        self.result[big_category_name] = {}
        for category_block in little_category_block:
            category_list = category_block.select('a')
            for category in category_list:
                name_category = category.text
                url_category = category.get('href')
                self.result[big_category_name][name_category] = url_category


        #logger.info('%s',url)
    def json_writer(self, dict):
        #json_object = json.dumps(dict)
        with open("categories.json", "w",encoding='utf-8') as outfile:
            json_object = json.dumps(dict,ensure_ascii=False)
            outfile.write(json_object)
    def timestamp_adding(self, res):
        cur_timestamp = int(round(datetime.now().timestamp()))

        res = {
            cur_timestamp:res
        }
        return res

    def run(self):
        text = self.load_page(self.url)
        self.parse_page(text)
        self.result = self.timestamp_adding(self.result)
        print(self.result)
        self.json_writer(self.result)
        return self.result


if __name__ == '__main__':
    parser = Client()
    parser.run()