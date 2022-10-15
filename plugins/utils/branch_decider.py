from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.engine import Engine

def is_data_exist(**kwargs):
    hook = PostgresHook(postgres_conn_id="taxi_trips_conn_id")
    engine: Engine = hook.get_sqlalchemy_engine()
    
    sql = kwargs['sql']
    print(f"Executing query: \n{sql}")

    with engine.connect() as conn:
        result = conn.execute(sql)
        count = result.first()[0] # get count value
        print(f"Rows count: {count}")

        if count == 0:
            print(f"Because the count is {count}. So we proceed to insert the data")
            return "insert_stg_raw_trip_data_to_prod" # task ID
        else:
            print(f"Because the count is more than 0. So we don't need to insert the data")
            return "no_insert_data" # task ID