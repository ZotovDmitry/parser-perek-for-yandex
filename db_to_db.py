#Тут я перевожу данные из одной таблицы (pg) в другую в другую базу данных (pg2)
from parser_client import Postgres_Client, Parser_Client
conn_params="host=localhost  dbname=airflow user=airflow password=airflow"
conn_params2="host=localhost dbname=postgres user=postgres password=docker port=5444"
pg2 = Postgres_Client(conn_params2)
pg = Postgres_Client(conn_params)
ps = Parser_Client()
import json
import time
from ast import literal_eval

def parse_content():
    #sql2 = """
    #DROP TABLE   fast_perek_datamart
    #"""
    #pg2.exexute_sql(sql2)
    sql2 = """
                        CREATE TABLE IF NOT EXISTS fast_perek_datamart (
                        url VARCHAR(255) NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        price NUMERIC(25,2) NOT NULL,
                        price_old NUMERIC(25,2),
                        start_timestamp VARCHAR(255) NOT NULL, 
                        weight_g NUMERIC(25,2),
                        price_g NUMERIC(25,2),
                        price_old_g NUMERIC(25,2));
                      """

    pg2.exexute_sql(sql2)
    sql2 = """select * from women_recipe"""
    pg2.exexute_sql(sql2)

    for record in pg2.content:
        print(record)

    sql = """
    select * from fast_perek_datamart
    """

    pg.exexute_sql(sql)

    for record in pg.content:
        print(record[0])

    # Эти адреса мы записываем в отдельный лист всех адресов

    #for record in pg.content:
    #    print(record[0])

    # Смотрим для каких продуктов мы отпарсили контент

        insert_script = ("""INSERT INTO fast_perek_datamart
                                                (url ,
                        name ,
                        price,
                        price_old,
                        start_timestamp ,
                        weight_g ,
                        price_g ,
                        price_old_g)
                        VALUES (%s, %s,%s,%s,%s,%s,%s,%s)""")

        insert_value = (record[0],
                        record[1],
                        record[2],
                        record[3],
                        record[4],
                        record[5],
                        record[6],
                        record[7])


        pg2.cursor.execute(insert_script, insert_value)
        pg2.conn.commit()

parse_content()
#parse_content()
#Сравниваем отпарсенные адреса со всеми и получаем список адресов, которые нам осталось отпарсить
"""
    for record in pg.content:
        parsed_links.append(record[0])
    for link in all_links:
        if link in parsed_links:
            pass
        else:
            links_to_parse.append(link)
"""