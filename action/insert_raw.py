# IMPORT MODULE
import json
import pandas as pd
import os
from connection.mysql import MySQL
from .open_credential import credential

credential = credential()
def insert_raw_data():
  mysql_auth = MySQL(credential['mysql_lake'])
  engine, engine_conn = mysql_auth.connect()
  script_dir = os.path.dirname(__file__)
  dir_file_data = os.path.join(script_dir, '../data/data_covid.json')
  with open (dir_file_data, "r") as data:
    data = json.load(data)

  df = pd.DataFrame(data['data']['content'])

  df.columns = [x.lower() for x in df.columns.to_list()]
  df.to_sql(name='dt_raw_covid', con=engine, if_exists="replace", index=False)
  engine.dispose()
  