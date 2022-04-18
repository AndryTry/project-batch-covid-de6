from action.insert_raw import insert_raw_data
from action.create_schema import create_star_schema
from action.insert_raw_warehouse import insert_raw_to_warehouse

if __name__ == '__main__':
  insert_raw_data()
  create_star_schema(schema='covid_de6')
  insert_raw_to_warehouse(schema='covid_de6')