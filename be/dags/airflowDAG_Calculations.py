import json
import time
import pendulum
import json
import pandas as pd
import requests
import redis
import sys
from redis.commands.timeseries.info import TSInfo
from redis.sentinel import Sentinel
from typing import List,Any
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from be.brompton.CalcUtils import *
from be.brompton.WorkerAllocator import *
from be.brompton.TSDBUtils import *

MAXBACKFILL=30*24*3600*1000  # 30 days
hook = PostgresHook('datalogger_postgres')
# tdsb_host_parts = Variable.get("tsdb_host").split(":")
tdsb_host_parts = Variable.get("tsdb_host").split(":")
print("######TSDB_HOST###########", tdsb_host_parts)
tsdb_host = tdsb_host_parts[0]
print("######TSDB_HOST###########", tsdb_host)
tsdb_port = int(tdsb_host_parts[1])
tsdb_pwd = None
try:
    tsdb_pwd = Variable.get("tsdb_password")
except:
    pass
tsdb_master = None
try:
    tsdb_master = Variable.get("tsdb_master")
except:
    pass
print("######TSDB_MASTER###########", tsdb_master)
# conn = redis.Redis(tdsb_host_parts[0], int(tdsb_host_parts[1]))
if (tsdb_master):
            sentinel = Sentinel(sentinels=[(tsdb_host, tsdb_port),
                                           ], socket_timeout=10, sentinel_kwargs={'password': tsdb_pwd},
                                password=tsdb_pwd)
            # sentinel = Sentinel(sentinels=[('redis-service', 26379),
            #       ],socket_timeout=10,sentinel_kwargs={'password': 'test@123'},password='test@123')
            conn = sentinel.master_for(tsdb_master)
else:
    conn = redis.Redis(tsdb_host, tsdb_port, password=tsdb_pwd)
api_url=Variable.get("api_url")

with DAG(
        dag_id="calc_engine",
        schedule_interval="*/1 * * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["calculations"],
        max_active_runs=1
) as dag:
    @task
    def allocate_workers(**context)-> List[List[Union[Dict[str, Any], OrderedDict[str, Any]]]]:
        offline_calcs=get_offline_calcs(hook=hook)
        # go through poll group and determine if needs to be scheduled, if so group by poll,instances
        exec_ts_secs=round(datetime.strptime(context['ts'],'%Y-%m-%dT%H:%M:%S%z').timestamp())
        exec_ts=round(exec_ts_secs/60)
        print(f"Dag's execution timestamp {exec_ts_secs}")
        candidates=[]
        print("it is time to execute the following calcs:")
        for poll_str,cust_instances in offline_calcs.items():
            poll=get_poll_in_minutes(poll_str)
            for cust, instances in cust_instances.items():
                if(exec_ts%poll==0):
                    print(f"customer:{cust}, poll:{poll_str}, instances:{instances}")
                    for instance_dtype in instances: # is list of tuples (instance,dtype)
                        # zero pad criteria so that instances 2 and 10001 are considered correctly (2 precedes 10001 but not in strings)
                        candidates.append(AllocateCandidate(instance_dtype[0],f"{int(1000000+poll)}_{int(1000000+len(instances))}",{'cust_id':cust,'ts':exec_ts,'poll':poll_str,'data_type':instance_dtype[1]}))
        allocated=allocate(candidates,max_items_per_worker=100,max_workers=50)
        return allocated
    @task
    def execute_calculations(allocated_calc_instances:List[dict]):
        # load into data frame to be able to group by customer and poll_period when making request
        df=pd.DataFrame(allocated_calc_instances)
        # expand metadata to columns
        df_meta = pd.DataFrame(df.pop('metadata').values.tolist(), columns=['cust_id', 'ts', 'poll','dtype'])
        df = df.join(df_meta)

        # read info in to determine the most recent timestamps
        ids=[]
        with conn.pipeline(transaction=False) as pipe:
            # raw data
            for row in df[['id','dtype']].itertuples():  # raw key is the volume key,
                if(row.dtype in ['STRING','BOOLEAN']):
# TODO: this is not the correct way to get information about redis streams
                    pipe.xinfo(row.id)
                else:
                    pipe.ts().info(row.id)
                ids.append(row.id)
            results = pipe.execute(raise_on_error=False)
        calcs_last_timestamps=[]
        for id,result in zip(ids,results):
            if (isinstance(result, redis.ResponseError)):
                print(f"ERROR: retrieving last timestamp from calculations tag: {id}: {result.args[0]}")
            else:
                if(isinstance(result,TSInfo)):
                    calcs_last_timestamps.append({'id':id,'last_ts':result.last_timestamp})
                elif('last-entry' in result):
                    ts=int(result['last-entry'][0].decode('utf-8')[:-2])
                    calcs_last_timestamps.append({'id':id,'last_ts':ts})

        print("calculations last_timestamps from tsdb:")
        print(calcs_last_timestamps)

        # join dataframes
        df_timestamps = pd.DataFrame(calcs_last_timestamps)
        df = df.merge(df_timestamps, right_on=['id'], left_on=['id'])

        # compute query paremeters
        now = 60 * 1000 * df['ts'].iloc[0]
        min_ts = now - MAXBACKFILL
        # process by type customer by customer and poll by poll
        results={'TS':[],'STREAM':[]} # numeric or not--> results
        for stype in list(results.keys()):  # 0 if streams and 1 if TS
            if (stype == 'STREAM'):
                criteria = df['dtype'].isin(['STRING', 'BOOLEAN'])
                print("requesting data from non-numerics - urls:")
            else:
                criteria = ~df['dtype'].isin(['STRING', 'BOOLEAN'])
                print("requesting data from numerics - urls:")
            unique_poll = df[criteria]['poll'].unique()
            for poll in unique_poll:
                unique_cust = df[(df['poll'] == poll) & (criteria)]['cust_id'].unique()
                for cust in unique_cust:
                    unique_last_ts = df[(df['poll'] == poll) & (df['cust_id'] == cust) & (criteria)]['last_ts'].unique()
                    for last_ts in unique_last_ts:
                        ids_list = ",".join(str(i) for i in
                                            df[(df['poll'] == poll) & (df['cust_id'] == cust) & (df['last_ts'] == last_ts) & (criteria)][
                                                'id'].values.tolist())
                        start_ts = max(last_ts, min_ts)
                        # Ask for data from the last ts stored (results.last_timestamp) or 30 days ago whichever is greater - passing utc so do not ask for asset timezone
                        url = f"{api_url}timeseries/agg/{cust}?meas_id={ids_list}&start={start_ts}&end={now}&agg=twa&agg_period={poll}&use_asset_tz=false"
                        print(url)
                        resp = requests.get(url, headers={'X-UsePersistedCalcs': 'false'})
                        if(resp.status_code==200):
                            obj_list = resp.json()
                            print(obj_list)
                            for obj in obj_list:
                                # organize response as tuple (k,t,v)
                                if ('ts,val' in obj):
                                    tvs = obj['ts,val']
                                    for tv in tvs:
                                        tupl=(obj['tag'],int(tv[0]),tv[1])
                                        results[stype].append(tupl)
                                elif ('error' in obj):
                                    print(f"ERROR: tag {obj['tag']} has error {obj['error']}")
                                else:
                                    print(f"tag {obj['tag']} did not return values")
                        else:
                            print(f"ERROR {resp.status_code}: retrieving values for url {url}:\n", resp.reason, file=sys.stderr)
        return results

    @task
    def store_calculations_in_tsdb(calc_results: Dict[str, List[Tuple[int,int,Any]]]):  # topic->messages
        for k,vals in calc_results.items():
            print(f"Calc {len(vals)} results received for {k}")
#        print(json.dumps(calc_results, indent=4))
        bulk_store_in_tsdb(conn, calc_results)

    # Main flow
    allocated = allocate_workers()
    calc_results=execute_calculations.expand(allocated_calc_instances=allocated)
    store_calculations_in_tsdb.expand(calc_results=calc_results)




