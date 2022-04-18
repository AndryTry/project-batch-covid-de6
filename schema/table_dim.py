def create_table_dim(schema):
  query = f"""
  CREATE TABLE IF NOT EXISTS {schema}.dim_province (
    province_id text primary key,
    province_name text);
  CREATE TABLE IF NOT EXISTS {schema}.dim_district (
      district_id text primary key,
      province_id text,
      district_name text);
  CREATE TABLE IF NOT EXISTS {schema}.dim_case (
      id SERIAL primary key,
      status_name text,
      status_detail text);
  """

  return query