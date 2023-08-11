import  requests
import logging
import collections
import bs4
import json
import re
import  time
import  lxml
import  os
import pandas as pd
import psycopg2




attributes = ['name','price','url','little_cat','big_cat','start_timestamp','shop','price_old']


variables = {
    'link_cat':'https://www.perekrestok.ru/cat/c/150/ovosi',
    'block_class':'div.sc-dlfnbm.ldVxnE',
    'url_one':'a.sc-dQoVA.fvoiIk.product-card__link',
    'get_url':'href',
    'img_url_one':'img.product-card__image',
    'get_img':'src',
    'name_one':'div.product-card__title',
    'get_name':' text',
    'price_one':'div.price-new',
    'price_old':'div.price-old',
    'get_price':'text'
}


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('wb')

ParseResult = collections.namedtuple(
    'ParseResult',
    (
        'name',
        'price',
        'price_old',
        'url',
        'little_cat',
        'big_cat'
    )
)





class Client:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Accept-Language':'ru'
        }
        self.result = {}
        self.df = pd.DataFrame(columns=attributes)

    def parse_whole_db(self,source):
        self.start_timestamp = list(source.keys())[0]
        source = source[self.start_timestamp]
        for big_cat,little_cat_dict in source.items():
            for little_cat in little_cat_dict:
                print(little_cat)
                print(big_cat)
                link_cat = 'https://www.perekrestok.ru'+little_cat_dict[little_cat]
                text = self.load_page(url = link_cat)
                self.parse_page(text=text,little_cat=little_cat,big_cat=big_cat)
                time.sleep(4)


    def load_page(self, url):
        while True:
            try:
                res = self.session.get(url=url)
                res.raise_for_status()
                text = res.text
                res.close()
                if len(text) < 10000:
                    raise ValueError('len of res.text is not enough')
                break
            except Exception as e:
                print('Exception ',e,'. Trying again...')
                time.sleep(20)
        return text

    def parse_page(self, text: str,little_cat,big_cat):
        soup = bs4.BeautifulSoup(text,'lxml')
        container = soup.select('div.sc-dWddBi.fwoXnq')
        for id,block in enumerate(container,start=1):
            self.parse_block(block=block,little_cat=little_cat,big_cat=big_cat,id=id)



##################################################################
    def parse_name(self, block, vars = {}):
        select_class = vars['name_one']
        try:
            name = block.select_one(select_class)
            name = name.text
            if not name:
                logger.error('no name')
                return None
        except:
            None
        return name

    def parse_url(self, block, vars = {}):
        select_class = vars['url_one']
        try:

            url_block = block.select_one(select_class)
            if not url_block:
                logger.error('no url_block')
                return 'no url'
            url = url_block.get('href')
            if not url:
                logger.error('no url')
                return 'no url'
        except:
            return None
        return url

    def parse_price(self, block, vars = {}):
        select_class = vars['price_one']
        try:
            price = block.select_one(select_class)
            price = price.text
        except:
            price = None
        if not price:
            logger.error('no name')
            return None
        return price

    def parse_old_price(self, block, vars = {}):
        select_class = vars['price_old']
        try:
            price = block.select_one(select_class)
            price = price.text
            print('old price is ',price)
        except:
            price = None
        #if not price:
            #logger.error('no name')
            #return 'no name'
        return price
##################################################################

    def parse_block(self, block, little_cat,big_cat,id):
        #logger.info(block)
        #logger.info('=' * 100)
        df = pd.DataFrame(columns=attributes)
        url = self.parse_url(block, variables)
        price = self.parse_price(block, variables)
        name = self.parse_name(block, variables)
        print(name)
        price_old = self.parse_old_price(block, variables)
        self.result[url]={
            'name':name,
            'price':price,
            'url':url,
            'little_cat':little_cat,
            'big_cat':big_cat,
            'start_timestamp':self.start_timestamp,
            'shop':'perekrestok',
            'price_old': price_old
        }

        self.add_dict_to_df(self.result[url])
        #print(df)
        #self.df.concat(df,  ignore_index=True)
        #print(self.df)
        #print(self.result[url])
        #logger.info('%s',url)

    def json_writer(self, res):
        with open("/opt/airflow/dags/products.json", "w",encoding='utf-8') as outfile:
            json_object = json.dumps(res,ensure_ascii=False)
            outfile.write(json_object)

    def add_dict_to_df(self, dict):
        row = list(dict.values())
        self.df.loc[len(self.df)] = row


    def run(self):
        with open('/opt/airflow/dags/categories.json') as json_file:
            categories_dict = json.load(json_file)
            #print(categories_dict)
        #time.sleep(10)
        self.parse_whole_db(source = categories_dict)
        self.json_writer(res = self.result)
        self.df.to_csv('/opt/airflow/dags/products_perek.csv')


if __name__ == '__main__':
    parser = Client()
    parser.run()

