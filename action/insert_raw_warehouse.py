import json
import pandas as pd
import os
from connection.mysql import MySQL
from connection.postgresql import PostgreSQL
from .insert_dim_table import insert_dim_province, insert_dim_district, insert_dim_case
from .insert_fact_table import insert_fact_district_monthly, insert_fact_district_yearly, insert_fact_province_daily, insert_fact_province_monthly, insert_fact_province_yearly
from .open_credential import credential

credential = credential()

def insert_raw_to_warehouse(schema):
    mysql_auth = MySQL(credential['mysql_lake'])
    engine, engine_conn = mysql_auth.connect()
    data = pd.read_sql(sql='dt_raw_covid', con=engine)
    engine.dispose()

    # filter needed column
    column = ["tanggal", "kode_prov", "nama_prov", "kode_kab", "nama_kab", "suspect_diisolasi", "suspect_discarded", "closecontact_dikarantina", "closecontact_discarded", "probable_diisolasi", "probable_discarded", "confirmation_sembuh", "confirmation_meninggal", "suspect_meninggal", "closecontact_meninggal", "probable_meninggal"]
    data = data[column]

    dim_province = insert_dim_province(data)
    dim_district = insert_dim_district(data)
    dim_case = insert_dim_case(data)

    fact_province_daily = insert_fact_province_daily(data, dim_case)
    fact_province_monthly = insert_fact_province_monthly(data, dim_case)
    fact_province_yearly = insert_fact_province_yearly(data, dim_case)
    fact_district_monthly = insert_fact_district_monthly(data, dim_case)
    fact_district_yearly = insert_fact_district_yearly(data, dim_case)

    postgre_auth = PostgreSQL(credential['postgresql_warehouse'])
    engine, engine_conn = postgre_auth.connect(conn_type='engine')

    dim_province.to_sql('dim_province', schema=schema, con=engine, index=False, if_exists='replace')
    dim_district.to_sql('dim_district', schema=schema, con=engine, index=False, if_exists='replace')
    dim_case.to_sql('dim_case', schema=schema, con=engine, index=False, if_exists='replace')

    fact_province_daily.to_sql('fact_province_daily', schema=schema, con=engine, index=False, if_exists='replace')
    fact_province_monthly.to_sql('fact_province_monthly', schema=schema, con=engine, index=False, if_exists='replace')
    fact_province_yearly.to_sql('fact_province_yearly', schema=schema, con=engine, index=False, if_exists='replace')
    fact_district_monthly.to_sql('fact_district_monthly', schema=schema, con=engine, index=False, if_exists='replace')
    fact_district_yearly.to_sql('fact_district_yearly', schema=schema, con=engine, index=False, if_exists='replace')

    engine.dispose()