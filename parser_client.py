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
import numpy as np
import psycopg2.extras as extras



class Parser_Client:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Accept-Language':'ru'
        }
        self.result = {}
        #self.df = pd.DataFrame(columns=attributes)


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
                time.sleep(60)
        return text

    def parse_list(self, text: str, tag):

        """
        input = text of page
        output = list of parsed blocks
        parse tags from sourse
        example of tag:
        tag = div.sc-dlfnbm.ldVxnE
        """

        soup = bs4.BeautifulSoup(text, 'lxml')
        container = soup.select(tag)
        return container

    def parse_block(self, block, tag, method='text'):
        try:
            block_content = block.select_one(tag)
            if method == 'text':
                block_content = block_content.text
            if method == 'href':
                block_content = block_content.get('href')
        except Exception as e:
            print(e)
            return None
        return block_content

    def parse_one_tag_from_str(self,text,tag):
        soup = bs4.BeautifulSoup(text, 'lxml')
        try:
            res = soup.select_one(tag).text
        except:
            return None
        return  res

    def json_writer(self, res, ):
        with open("products.json", "w",encoding='utf-8') as outfile:
            json_object = json.dumps(res,ensure_ascii=False)
            outfile.write(json_object)

class Postgres_Client():
    def __init__(self, conn_params="host=postgres dbname=airflow user=airflow password=airflow"):
        self.conn = psycopg2.connect(
            conn_params
        )
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


    def execute_values(self,conn, df, table):
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ','.join(list(df.columns))
        # SQL query to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = self.cursor
        try:
            extras.execute_values(cursor, query, tuples)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("the dataframe is inserted")
        cursor.close()

    def csv_to_postgre(self,csv_path):
        df = pd.read_csv(csv_path, index_col=0)
        self.execute_values(self.conn, df, 'products_perek')

    def exexute_sql(self, sql):
        self.cursor.execute(sql)
        """
        self.content - в него записываются результаты селекта
        """
        self.conn.commit()
        try:
            self.content = self.cursor.fetchall()
        except:
            self.content = None

    def show(self):
        try:
            self.content = self.cursor.fetchall()
        except:
            self.content = None
        print(self.content)
        print("Total rows are:  ", len(self.content))

    def close_conn(self):
        if self.cursor is not None:
            self.cursor.close()
        if self.conn is not None:
            self.conn.close()