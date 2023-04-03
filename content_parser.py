from parser_client import Postgres_Client, Parser_Client
conn_params="host=localhost  dbname=airflow user=airflow password=airflow"
pg = Postgres_Client(conn_params)
ps = Parser_Client()
import json
import time
from ast import literal_eval
def parse_content():
    all_links = []
    parsed_links = []
    links_to_parse = []

    #Создаем таблицу с контентом

    sql ="""
    CREATE TABLE IF NOT EXISTS products_perek_content(
        id SERIAL PRIMARY KEY,
        url VARCHAR(255) NOT NULL,
        content TEXT
        );
    """
    pg.exexute_sql(sql)

    # Загружаем уже отпарсенные ранее адреса на продукты

    sql = """
    SELECT distinct url FROM products_perek
    """
    pg.exexute_sql(sql)

    # Эти адреса мы записываем в отдельный лист всех адресов

    for record in pg.content:
        all_links.append(record[0])

    # Смотрим для каких продуктов мы отпарсили контент

    sql = """
        SELECT distinct url FROM products_perek_content 
        --WHERE content is not null
        """
    pg.exexute_sql(sql)

    # Сравниваем отпарсенные адреса со всеми и получаем список адресов, которые нам осталось отпарсить

    for record in pg.content:
        parsed_links.append(record[0])
    for link in all_links:
        if link in parsed_links:
            pass
        else:
            links_to_parse.append(link)

    print('Number of all links is ' + str(len(all_links)))
    print('Number of parsed links is ' + str(len(parsed_links)))
    print('Number needed to be parsed is '+ str(len(links_to_parse)))
    count_of_all_links = str(len(links_to_parse))
    # Начинаем парсить контент для оставшихся адресов

    for count, link in enumerate(links_to_parse):
        print('loading '+str(count)+' out of  '+count_of_all_links)
        time.sleep(3)
        if count % 100 == 0 and count != 0:
            time.sleep(30)
        try:
            url = 'https://www.perekrestok.ru'+link
            text = ps.load_page_dynamic(url=url)
            res = ps.parse_list(text=text, tag='script')
            num = 0
            for number, element in enumerate(res):
                if str.__contains__(str(element), "window.__INITIAL_STATE__ = "):
                    print(element)
                    num = number
                    break
            json_acceptable_string = res[num].string.replace("'", "\"").replace('window.__INITIAL_STATE__ = ', '')
            countent_dict = json.loads(json_acceptable_string)
            countent_string = str(countent_dict)
        except:
            countent_string = None
            print('fail on number' + str(count))
            print(url)

        insert_script = ("""INSERT INTO products_perek_content
                            (url,  content)
                            VALUES (%s, %s)""")

        try:
            insert_value = (link,countent_string)
            pg.cursor.execute(insert_script,insert_value)
            pg.conn.commit()
        except:
            insert_value = (link, None)
            pg.cursor.execute(insert_script, insert_value)
            pg.conn.commit()

    #pg.close_conn()

def parse_json_content():
    """
            CREATE TABLE IF NOT EXISTS products_perek_composition(
                id SERIAL PRIMARY KEY,
                url VARCHAR(255) NOT NULL,
                gross_weight VARCHAR(255),
                net_weight VARCHAR(255),
                composition TEXT,
                protein VARCHAR(255),
                fat VARCHAR(255),
                carbs VARCHAR(255),
                country VARCHAR(255),
                brand VARCHAR(255),
                energy_value VARCHAR(255),

                );
            """
    sql = """
            DROP TABLE IF EXISTS products_perek_composition;
    """
    pg.exexute_sql(sql)
    sql = """
            CREATE TABLE IF NOT EXISTS products_perek_composition(
                id SERIAL PRIMARY KEY,
                url VARCHAR(255) NOT NULL,
                gross_weight VARCHAR(255),
                net_weight VARCHAR(255),
                composition TEXT,
                protein VARCHAR(255),
                fat VARCHAR(255),
                carbs VARCHAR(255),
                energy_value VARCHAR(255),
                manufacturer VARCHAR(255),
                brand VARCHAR(255),
                product_type VARCHAR(255),
                country VARCHAR(255))
                ;
            """

    pg.exexute_sql(sql)
    sql = """
            SELECT url,content 
            FROM (select *, row_number(*) over (partition by url order by id desc) rn from products_perek_content 
            WHERE content is not null) t1
            where rn = 1
            """
    pg.exexute_sql(sql)
    for record in pg.content:
        print(record[0])
        composition = None
        protein = None
        fat = None
        carbs = None
        energy_value = None
        gross_weight = None
        net_weight = None
        manufacturer = None
        brand = None
        product_type = None
        country = None
        json_string = record[1]
        json_dict = literal_eval(json_string)
        try:
            first_key = next(iter(json_dict['catalog']['productData']))
            features = json_dict['catalog']['productData'][first_key]['features']
            for item in features:
                #print(item['title'])
                if item['title'] == 'Состав':
                    try:
                        composition = item['items'][0]['displayValues'][0]
                    except:
                        pass
                if item['title'] == 'Пищевая ценность на 100г':
                    for sub_item in item['items']:
                        if sub_item['title'] == 'Ккал':
                            try:
                                energy_value = sub_item['displayValues'][0]
                            except:
                                pass
                        if sub_item['title'] == 'Белки':
                            try:
                                protein = sub_item['displayValues'][0]
                            except:
                                pass
                        if sub_item['title'] == 'Жиры':
                            try:
                                fat = sub_item['displayValues'][0]
                            except:
                                pass
                        if sub_item['title'] == 'Углеводы':
                            try:
                                carbs = sub_item['displayValues'][0]
                            except:
                                pass
                if item['title'] == 'Информация':
                    for info_item in item['items']:
                        if info_item['title'] == 'Вес (брутто)':
                            try:
                                gross_weight = info_item['displayValues'][0]
                            except:
                                pass
                        if info_item['title'] == 'Вес (нетто)':
                            try:
                                net_weight = info_item['displayValues'][0]
                            except:
                                pass
                        if info_item['title'] == 'Производитель':
                            try:
                                manufacturer = info_item['displayValues'][0]
                            except:
                                pass
                        if info_item['title'] == 'Торговая марка':
                            try:
                                brand = info_item['displayValues'][0]
                            except:
                                pass
                        if info_item['title'] == 'Тип продукта':
                            try:
                                product_type = info_item['displayValues'][0]
                            except:
                                pass
                        if info_item['title'] == 'Страна':
                            try:
                                country = info_item['displayValues'][0]
                            except:
                                pass
            print(
                composition,
            protein,
            fat,
            carbs,
            energy_value,
            gross_weight,
            net_weight,
            manufacturer,
            brand,
            product_type,
            country
            )

            insert_script = ("""INSERT INTO products_perek_composition
                                        (url ,
                gross_weight ,
                net_weight,
                composition,
                protein ,
                fat ,
                carbs ,
                energy_value ,
                manufacturer ,
                brand ,
                product_type ,
                country )
                                        VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""")

            try:
                insert_value = (record[0] ,
                gross_weight ,
                net_weight,
                composition,
                protein ,
                fat ,
                carbs ,
                energy_value ,
                manufacturer ,
                brand ,
                product_type ,
                country)
                pg.cursor.execute(insert_script, insert_value)
                pg.conn.commit()
            except:
                insert_value = (record[0], None, None, None, None, None, None, None, None, None, None, None)
                pg.cursor.execute(insert_script, insert_value)
                pg.conn.commit()

            print(features)

        except Exception as e:
            print(e)
    pg.exexute_sql(sql)
if __name__ == "__main__":
    parse_content()
    parse_json_content()
    pg.close_conn()