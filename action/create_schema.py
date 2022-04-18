from connection.postgresql import PostgreSQL
from schema.table_dim import create_table_dim
from schema.table_fact import create_table_fact
from .open_credential import credential

credential = credential()
def create_star_schema(schema):
  postgre_auth = PostgreSQL(credential['postgresql_warehouse'])
  conn, cursor = postgre_auth.connect(conn_type='cursor')

  query_dim = create_table_dim(schema=schema)
  cursor.execute(query_dim)
  conn.commit()

  query_fact = create_table_fact(schema=schema)
  cursor.execute(query_fact)
  conn.commit()

  cursor.close()
  conn.close()