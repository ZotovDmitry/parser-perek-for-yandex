import datetime
import psycopg2
import numpy as np
import psycopg2.extras as extras
import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
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
    dag_id='dag_parser_perek',
    schedule_interval='0 */12 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=180),
    tags=['example', 'example2'],
    params={"example_key": "example_value"},
) as dag:
    create_sql_function = PostgresOperator(
        task_id="create_sql_function",
        postgres_conn_id='postgres_localhost',
        sql="""
                    CREATE OR REPLACE FUNCTION public.isnumeric(text)
                     RETURNS boolean
                     LANGUAGE plpgsql
                     IMMUTABLE STRICT
                    AS $function$
                    DECLARE x NUMERIC;
                    BEGIN
                        x = $1::NUMERIC;
                        RETURN TRUE;
                    EXCEPTION WHEN others THEN
                        RETURN FALSE;
                    END;
                    $function$
                    ;
                  """
    )
    create_products_table = PostgresOperator(
        task_id="create_products_table",
        postgres_conn_id='postgres_localhost',
        sql="""
                    CREATE TABLE IF NOT EXISTS products_perek (
                    name VARCHAR(255) NOT NULL,
                    price VARCHAR(255) NOT NULL,
                    price_old VARCHAR(255) NOT NULL,
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

    content_parser = BashOperator(
        task_id='content_parser',
        bash_command='python /opt/airflow/dags/content_parser.py')

    to_postgre = PythonOperator(
        task_id = 'csv_to_postgre',
        python_callable =  csv_to_postgre

    )

    create_dirty_products_perek = PostgresOperator(
        task_id="create_dirty_products_perek",
        postgres_conn_id='postgres_localhost',
        sql="""
                        CREATE TABLE IF NOT EXISTS dirty_products_perek (
                        url VARCHAR(255) NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        weight FLOAT(8),
                        weight_string TEXT,
                        price_dirty VARCHAR(255),
                        price_old_dirty VARCHAR(255),
                        little_cat VARCHAR(255),
                        big_cat VARCHAR(255),
                        shop VARCHAR(255),
                        start_timestamp VARCHAR(255) NOT NULL, 
                        dirty_weight TEXT,
                        price_old FLOAT(8),
                        price FLOAT(8),
                        prvalue FLOAT(8),
                        pr_old_value FLOAT(8),
                        weight_g FLOAT(8));
                      """
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
            case when weight_string='' and quantity = 'кг' then 1 
            when weight_string='' and quantity = 'л' then 1 
            when quantity not in ('кг','г', 'л', 'мл') then null
            WHEN isnumeric(weight_string) THEN CAST(weight_string AS float) 
            ELSE null end as weight,*
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

    create_pure_products_perek = PostgresOperator(
        task_id="create_pure_products_perek",
        postgres_conn_id='postgres_localhost',
        sql="""
                            CREATE TABLE IF NOT EXISTS pure_products_perek (
                            url VARCHAR(255) NOT NULL,
                            name VARCHAR(255),
                            price FLOAT(8),
                            price_old FLOAT(8),
                            price_g FLOAT(8),
                            pr_old_g FLOAT(8),
                            weight_g FLOAT(8),
                            little_cat VARCHAR(255),
                            big_cat VARCHAR(255),
                            shop VARCHAR(255),
                            start_timestamp VARCHAR(255) NOT NULL);
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
                or big_cat like '%Колбас%' or big_cat like '%колбас%'
                or big_cat like '%Соус%' or big_cat like '%соус%'
                or big_cat like '%кетчуп%' or big_cat like '%Кетчуп%'
                or big_cat like '%Майонез%' or big_cat like '%майонез%') t1 where rn = 1
                            
                          """
    )

    create_dirty_products_perek_composition = PostgresOperator(
        task_id="create_dirty_products_perek_composition",
        postgres_conn_id='postgres_localhost',
        sql="""
                                CREATE TABLE IF NOT EXISTS dirty_products_perek_composition (
                                    weight_g FLOAT(8),
                                    weight_dirty FLOAT(8),
                                    fat_g FLOAT(8),
                                    carb_g FLOAT(8),
                                    protein_g FLOAT(8),
                                    protein_g_dirty FLOAT(8),
                                    fat_g_dirty FLOAT(8),
                                    carb_g_dirty FLOAT(8),
                                    energy_value_float FLOAT(8),
                                    quantity TEXT,
                                    id INTEGER NOT NULL,
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
                                    country VARCHAR(255));
                              """
    )

    insert_dirty_products_perek_composition = PostgresOperator(
        task_id="insert_dirty_products_perek_composition",
        postgres_conn_id='postgres_localhost',
        sql="""
                    insert into dirty_products_perek_composition
                    select 
                    case when weight_dirty > 10001 then weight_dirty/1000 else weight_dirty end as weight_g, * from ( 
                    select case when quantity = 'кг' then cast(replace(replace(replace(gross_weight,quantity,''),' ',''),' ','') as float)*1000
                    else null end as weight_dirty,
                    case when fat_g_dirty > 100 and product_type not like '%Масло%' and product_type not like '%масло%' then null 
                    when fat_g_dirty > 100 and (product_type like '%Масло%' or product_type like '%масло%') then 100
                    else fat_g_dirty end as fat_g,
                    case when carb_g_dirty > 100 and product_type not like '%Сахар%' and product_type not like '%сахар%' then null 
                    when carb_g_dirty > 100 and (product_type  like '%Сахар%' or product_type like '%сахар%') then 100
                    when carb_g_dirty > 95 and (product_type  like '%Масло%' or product_type like '%масло%') then 0
                     else carb_g_dirty end as carb_g,
                     protein_g_dirty as protein_g,
                    * from ( 
                    select cast(REPLACE(protein, ' г', '') as float) as protein_g_dirty,
                    cast(REPLACE(fat, ' г', '') as float) as fat_g_dirty,
                    cast(REPLACE(carbs, ' г', '') as float) as carb_g_dirty,
                    cast(energy_value as float) as energy_value_float,
                    REGEXP_REPLACE(gross_weight ,'[[:digit:]]|[^a-zA-Z0-9а-яА-Я_]','','g') as quantity,
                    * from 
                    (select * from products_perek_composition
                    where url not in 
                    (select distinct url from dirty_products_perek_composition)) src
                     ) t1
                     ) t2
                     order by protein_g_dirty desc nulls last
                              """
    )

    create_pure_products_perek_composition = PostgresOperator(
        task_id="create_pure_products_perek_composition",
        postgres_conn_id='postgres_localhost',
        sql="""
                                    CREATE TABLE IF NOT EXISTS pure_products_perek_composition (
                                    id INTEGER NOT NULL,
                                    url VARCHAR(255) NOT NULL,
                                    weight_g FLOAT(8),
                                    protein_g FLOAT(8),
                                    fat_g FLOAT(8),
                                    carb_g FLOAT(8),
                                    energy_value FLOAT(8),
                                    composition TEXT,
                                    manufacturer VARCHAR(255),
                                    brand VARCHAR(255),
                                    product_type VARCHAR(255),
                                    country VARCHAR(255));
                                  """
    )

    insert_pure_products_perek_composition = PostgresOperator(
        task_id="insert_pure_products_perek_composition",
        postgres_conn_id='postgres_localhost',
        sql="""
                         insert into pure_products_perek_composition
                         select id, url, weight_g, protein_g, fat_g, carb_g, energy_value_float as energy_value, composition,
                         manufacturer, brand, product_type, country from 
                        (select * from dirty_products_perek_composition dppc 
                        where url not in 
                        (select distinct url from pure_products_perek_composition)) src 
                                  """
    )

    create_pure_perek_weights = PostgresOperator(
        task_id="create_pure_perek_weights",
        postgres_conn_id='postgres_localhost',
        sql="""
                                        CREATE TABLE IF NOT EXISTS pure_perek_weights (
                                        url VARCHAR(255) NOT NULL,
                                        weight_g FLOAT(8) );
                                      """
    )

    insert_pure_perek_weights = PostgresOperator(
        task_id="insert_pure_perek_weights",
        postgres_conn_id='postgres_localhost',
        sql="""
                    insert into pure_perek_weights
                    select url,weight_g from (  
                    select row_number(*) over (partition by url order by weight_g nulls last) rn, url,weight_g from( 
                    select distinct url, 
                    case when weight_t1 is null and weight_t2 is not null then weight_t2
                    when weight_t1 > 10000 and  (big_cat like '%Напитки%' or big_cat like '%напитки%' or big_cat like '%алког%' or big_cat like '%Алког%') 
                    then weight_t1  / 1000
                    when weight_t1 is not null and weight_t2 is not null then weight_t1
                    else weight_t1 end  as weight_g
                    from 
                    (select * from 
                    (select t1.url,t1.big_cat, t1.weight_g as weight_t1, t2.weight_g as weight_t2   from pure_products_perek t1 
                    left join pure_products_perek_composition t2
                    on t1.url = t2.url) t1
                    where url not in 
                    (select distinct url from pure_perek_weights)) src
                    order by weight_g desc nulls last ) t1 ) t2 where rn = 1
                                      """
    )

    create_fast_perek_datamart = PostgresOperator(
        task_id="create_fast_perek_datamart",
        postgres_conn_id='postgres_localhost',
        sql="""
                                            CREATE TABLE IF NOT EXISTS fast_perek_datamart (
                                            url VARCHAR(255) NOT NULL,
                                            name VARCHAR(255)NOT NULL,
                                            price FLOAT(8),
                                            price_old FLOAT(8),
                                            start_timestamp VARCHAR(255) NOT NULL, 
                                            weight_g FLOAT(8),
                                            price_g FLOAT(8),
                                            price_old_g FLOAT(8));
                                          """
    )

    fast_perek_datamart = PostgresOperator(
        task_id="fast_perek_datamart",
        postgres_conn_id='postgres_localhost',
        sql="""
                    insert into fast_perek_datamart
                    select t1.url, t1.name, t1.price, t1.price_old, t1.start_timestamp,  t2.weight_g,
                    (t1.price/nullif(t2.weight_g,0)) as price_g, (t1.price_old /nullif(t2.weight_g,0)) as price_old_g
                    from (select * from pure_products_perek
                    where start_timestamp not in (select distinct start_timestamp from fast_perek_datamart)) t1
                    left join pure_perek_weights t2 on t1.url  = t2.url 
                                      """
    )

    create_long_perek_datamart = PostgresOperator(
        task_id="create_long_perek_datamart",
        postgres_conn_id='postgres_localhost',
        sql="""
                                                CREATE TABLE IF NOT EXISTS long_perek_datamart (
                                                url VARCHAR(255) NOT NULL,
                                                name VARCHAR(255) NOT NULL,
                                                weight_g FLOAT(8),
                                                protein_100g FLOAT(8),
                                                fat_100g FLOAT(8),
                                                carb_100g FLOAT(8),
                                                energy_value_100g VARCHAR(255), 
                                                composition TEXT,
                                                manufacturer VARCHAR(255),
                                                brand VARCHAR(255),
                                                product_type VARCHAR(255),
                                                country VARCHAR(255),
                                                shop VARCHAR(255),
                                                rn INTEGER);
                                              """
    )

    long_perek_datamart = PostgresOperator(
        task_id="long_perek_datamart",
        postgres_conn_id='postgres_localhost',
        sql="""
                        insert into long_perek_datamart
                        select * from ( 
                        select distinct t1.url, t1.name, t3.weight_g, t2.protein_g as protein_100g, t2.fat_g as fat_100g, t2.carb_g as carb_100g,t2.energy_value as energy_value_100g,
                        t2.composition, t2.manufacturer, t2.brand, t2.product_type, t2.country, t1.shop, 
                        row_number(*) over (partition by t1.url order by t3.weight_g nulls last) rn 
                        from 
                        (select * from pure_products_perek
                        where url not in (select distinct url from long_perek_datamart)) t1 
                        left join pure_products_perek_composition t2 on t1.url = t2.url 
                        left join pure_perek_weights t3 on t1.url  = t3.url ) t1 where rn = 1
                                          """
    )
    # [END howto_operator_bash_template]
    create_products_table >> category_parser
    category_parser >> products_parser >> to_postgre >> create_dirty_products_perek >> insert_dirty_products_perek >> create_pure_products_perek >> insert_pure_products_perek >> create_pure_perek_weights >>insert_pure_perek_weights
    to_postgre >> content_parser >> create_dirty_products_perek_composition >> insert_dirty_products_perek_composition >> create_pure_products_perek_composition >> insert_pure_products_perek_composition >> create_pure_perek_weights >> insert_pure_perek_weights
    insert_pure_perek_weights >> create_fast_perek_datamart >> fast_perek_datamart
    insert_pure_perek_weights >> create_long_perek_datamart >> long_perek_datamart
    create_sql_function

if __name__ == "__main__":
    dag.cli()
