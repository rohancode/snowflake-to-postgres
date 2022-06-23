from snowflake.sqlalchemy import URL

from sqlalchemy import create_engine
import sqlalchemy as sa

import pandas as pd
from datetime import datetime

import config


snowflake_engine = create_engine(URL(
    user = config.snowflake_user,
    password = config.snowflake_password,
    account = config.snowflake_account,
    warehouse = config.snowflake_warehouse,
    database = config.snowflake_database,
    schema = config.snowflake_schema
))
snowflake_connection = snowflake_engine.connect()

postgres_engine = create_engine(config.postgres_url)
postgres_connection = postgres_engine.connect()


inspector = sa.inspect(snowflake_engine)
unload_schema_name = inspector.default_schema_name
unload_table_names = inspector.get_table_names(unload_schema_name)


schema_start_name = unload_schema_name + '_new_' + str(datetime.utcnow()).replace('-','_').replace(' ','_').replace(':','_').replace('.','_')

postgres_sql = f'''
CREATE SCHEMA {schema_start_name};
'''
postgres_connection.execute(postgres_sql)


for table in unload_table_names:
    
    print("syncing table", table)
    
    query = f'''
        select * from {table};
        '''
    df = pd.read_sql(query, snowflake_connection)

    df.to_sql(table, 
          postgres_engine, 
          schema=schema_start_name, 
          method='multi',
          if_exists="fail",
          chunksize=100000)


schema_final_name = unload_schema_name + '_' + str(datetime.utcnow()).replace('-','_').replace(' ','_').replace(':','_').replace('.','_')

postgres_sql = f'''
ALTER SCHEMA {schema_start_name} RENAME TO {schema_final_name};
'''
postgres_connection.execute(postgres_sql)
print("schema saved with name:", schema_final_name)


snowflake_connection.close()
postgres_connection.close()