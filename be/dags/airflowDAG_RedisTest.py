import pendulum
from airflow import DAG
from airflow.decorators import task
import random
from datetime import datetime
import redis

redis_key = "write_test_666"

with DAG(
        dag_id="redids_write_test",
        schedule_interval="*/1 * * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["test", "redis"],
        max_active_runs=10
) as dag:
    @task
    def write_to_redis(pool):
        ts=round(datetime.now().timestamp()*1000)
        conn=redis.Redis(connection_pool=pool)
        # Create key if no exist
        try:
            info=conn.ts().info(redis_key)
            print(f"writing to {redis_key} with info {info}")
        except Exception as e:
            if ("key does not exist" in str(e)):
                print(f"WARNING - Timeseries {redis_key} does not exist - creating...")
                conn.ts().create(redis_key)
                conn.ts().alter(redis_key, retention_msecs=round(7*24*3600*1000))  # 7 days retention
            else:
                raise e
        # write random value
        random.seed(ts)
        val=random.random()
        conn.ts().add(redis_key,ts,val)

    # Main flow
    pool = redis.ConnectionPool(host="redis-service", port=26379, db=0, password="test@123")
    write_to_redis(pool)




