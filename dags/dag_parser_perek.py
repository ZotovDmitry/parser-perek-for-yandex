import datetime
import psycopg2
import numpy as np
import psycopg2.extras as extras
import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from category_parser import Client
from airflow.operators.postgres_operator import PostgresOperator



def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
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

def csv_to_postgre():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    df = pd.read_csv('/opt/airflow/dags/products_perek.csv', index_col=0)
    execute_values(conn, df, 'products_perek')

with DAG(
    dag_id='dagg_perek',
    schedule_interval='0 */12 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
) as dag:
    create_products_table = PostgresOperator(
        task_id="create_products_table",
        postgres_conn_id='postgres_localhost',
        sql="""
                    CREATE TABLE IF NOT EXISTS products_perek (
                    name VARCHAR(255) NOT NULL,
                    price VARCHAR(255)NOT NULL,
                    url VARCHAR(255) NOT NULL,
                    little_cat VARCHAR(255) NOT NULL,
                    big_cat VARCHAR(255) NOT NULL,
                    start_timestamp VARCHAR(255) NOT NULL, 
                    shop VARCHAR(255) NOT NULL);
                  """
    )
    category_parser = BashOperator(
        task_id='category_parser',
        bash_command='python /opt/airflow/dags/category_parser.py')

    products_parser = BashOperator(
        task_id='products_parser',
        bash_command='python /opt/airflow/dags/parser.py')

    to_postgre = PythonOperator(
        task_id = 'csv_to_postgre',
        python_callable =  csv_to_postgre

    )
    insert_dirty_products_perek = PostgresOperator(
        task_id="insert_dirty_products_perek",
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into dirty_products_perek
            select url,name,weight, weight_string,price as price_dirty,price_old as price_old_dirty, little_cat, big_cat,shop,start_timestamp,dirty_weight,
             case when pr_old = 'NaN' then null else pr_old end as price_old,
             case when pr = 'NaN' then null else pr end as price,
             case when prvalue = 'NaN' then null else prvalue end as prvalue,
              case when pr_old_value = 'NaN' then null else pr_old_value end as pr_old_value,
              case when weight_g = 'NaN' then null else weight_g end as weight_g
             from ( select pr/nullif(weight_g,0) as prvalue, 
              pr_old/nullif(weight_g,0) as pr_old_value, 
            *  from ( select *, case when quantity = 'кг' or quantity = 'л'
            then (weight*1000) else weight end as weight_g
            from(
            select 
            case WHEN isnumeric(weight_string) THEN CAST(weight_string AS float) 
            when weight_string='' and quantity = 'кг' then 1 
            when weight_string='' and quantity = 'л' then 1 ELSE null end as weight,*
            from(
            select replace(replace(replace(dirty_weight,quantity,''),' ',''),' ','') as weight_string , *
            from (select   REGEXP_REPLACE(dirty_weight,'[[:digit:]]|[^a-zA-Z0-9а-яА-Я_]','','g') quantity , *  from 
            (select *, cast(replace(REPLACE(REPLACE(REPLACE(price, '₽', ''),',', '.'),' ',''),' ','') as float)  pr,
            cast(replace(REPLACE(REPLACE(REPLACE(price_old, '₽', ''),',', '.'),' ',''),' ','') as float)  pr_old,
            split_part(name, ',', 2) as dirty_weight
            from 
            ( 
            select * from products_perek pp 
            where start_timestamp not  in 
            (select distinct start_timestamp from dirty_products_perek dpp )
            ) src
            order by pr desc) t1) t2 ) t3 ) t4) t5) t6
            order by weight_g desc nulls last
            ;
                      """
    )
    insert_pure_products_perek = PostgresOperator(
        task_id="insert_pure_products_perek",
        postgres_conn_id='postgres_localhost',
        sql="""
                insert into pure_products_perek
                select url, name, price, price_old, prvalue as price_g, pr_old_value as pr_old_g,weight_g, 
                little_cat, big_cat, shop, start_timestamp 
                from (select * , row_number(*)  over (partition by url, start_timestamp order by weight desc nulls last) rn 
                from (select *  from dirty_products_perek
                where start_timestamp not  in 
                (select distinct start_timestamp from pure_products_perek
                )
                order by start_timestamp desc) as src 
                where big_cat like '%Овощ%' or big_cat like '%овощ%' or big_cat like '%Фрукт%' or big_cat like '%фрукт%' or big_cat like '%Гриб%' or big_cat like '%гриб%'
                or big_cat like '%Орех%' or big_cat like '%орех%' or big_cat like '%Сем%' or big_cat like '%сем%'
                or big_cat like '%рыб%' or big_cat like '%Рыб%' or big_cat like '%Мореп%' or big_cat like '%мореп%'
                or big_cat like '%Консерв%' or big_cat like '%консерв%' or big_cat like '%выпеч%' or big_cat like '%Выпеч%'
                or  big_cat like '%моро%' or big_cat like '%Моро%'
                or big_cat like '%алко%' or big_cat like '%Алко%'
                or big_cat like '%Чипс%' or big_cat like '%чипс%' or big_cat like '%Снек%' or big_cat like '%снек%'
                or big_cat like '%Хлеб%' or big_cat like '%хлеб%' or big_cat like '%Готов%' or big_cat like '%готов%'
                or big_cat like '%Мяс%' or big_cat like '%мяс%' or big_cat like '%птиц%' or big_cat like '%Птиц%'
                or big_cat like '%деликатес%' or big_cat like '%Деликатес%' or big_cat = '%Кофе%' or big_cat = '%кофе%' or big_cat = '%Чай%' or big_cat = '%чай%'
                or big_cat like '%Сахар%' or big_cat like '%сахар%' or big_cat like '%Какао%' or big_cat like '%какао%' or big_cat like '%Кофе%'
                or big_cat like '%вод%' or big_cat like '%Вод%' or big_cat like '%Сок%'  or big_cat like '%сок%' 
                or big_cat like '%Шоколад%' or big_cat like '%шоколад%' or big_cat like '%Конфет%' or big_cat like '%конфет%'
                or big_cat like '%Молок%' or big_cat like '%молок%' or big_cat like '%молоч%' or big_cat like '%Молоч%'
                or big_cat like '%сыр%' or big_cat like '%Сыр%' or big_cat like '%Яйц%' or big_cat like '%яйц%'
                or big_cat like '%Мёд%' or big_cat like '%мёд%' or big_cat like '%Макарон%' or big_cat like '%макарон%'
                or big_cat like '%круп%' or big_cat like '%Круп%' or big_cat like '%масл%' or big_cat like '%Масл%'
                or big_cat like '%Колбас%' or big_cat like '%колбас%') t1 where rn = 1
                            
                          """
    )




    # [END howto_operator_bash_template]
    create_products_table >> category_parser
    category_parser >> products_parser >> to_postgre >> insert_dirty_products_perek >> insert_pure_products_perek




if __name__ == "__main__":
    dag.cli()
