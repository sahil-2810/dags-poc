import pendulum
import pandas as pd
from airflow import DAG
from airflow.decorators import task
import random
import math
from typing import List,Union,Any,Dict,OrderedDict
from datetime import datetime
from collections import namedtuple
import logging

logger = logging.getLogger(__name__)
logger.info("Helloi From Mario's DAG")

# Marshall to known type
AllocateCandidate = namedtuple("AllocateCandidate", "id measure metadata")

def allocate(items:List[AllocateCandidate], max_workers=10, max_items_per_worker=120)\
        -> List[List[Union[Dict[str, Any], OrderedDict[str, Any]]]]:  # [[AllocateCandidate as OrderedDict for the benefit of airflow]]
    items.sort(key=lambda i: i.measure) # prioritize by measure ascending
    total_items=len(items)
    total_cnt = 0
    worker_cnt = 0
    allocated_items=[]
    target_items=min(total_items, (max_workers * max_items_per_worker))
    start=0
    while(total_cnt<target_items and worker_cnt<max_workers):
        end=min(start+max_items_per_worker,total_items)
        worker_items=items[start:end]
        allocated_items.append([item._asdict() for item in worker_items])  # convert to dictionary for airflow's benefit
        total_cnt += len(worker_items)
        worker_cnt+=1
        start = end
    return allocated_items

def calculate(a,b,expr):
    return eval(expr)
def generate_test_calcs(n: int, divisor: int):
    if (n < 2):
        n = 2
    ret = []
    if (not (n // divisor)):
        divisor = n+1
    offset = n // divisor
    total = offset + round(n * (divisor - 1) * random.random() / divisor)
    for i in range(total):
        item = AllocateCandidate(id=f"calc{i}", measure=datetime.now().timestamp() - 10 * random.random(),
                                 metadata={"a": random.randint(0, n),"b":random.randint(0, n),"expression":"a+b" if random.random()<0.5 else "a*b"})
        ret.append(item)
    return ret

# TODO: this can be moved to config variables so that it can be controller
n = 10  # max number of calcs to do
m = 2  # calcs per worker

d = 3 # minimum fraction divisor

with DAG(
        dag_id="worker_allocator_test_simple_v1",
        schedule_interval="*/2 * * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["test","demo"],
        max_active_runs=10
) as dag:
    @task
    def allocate_workers(**context)-> List[List[Union[Dict[str, Any], OrderedDict[str, Any]]]]:
        items=generate_test_calcs(n,d)
        allocated=allocate(items,max_items_per_worker=m,max_workers=math.ceil(n/m))
        return allocated
    @task
    def execute_calculations(allocated_calc_instances:List[dict])->List[List[Any]]:
        # load into data frame to be able to group by customer and poll_period when making request
        df=pd.DataFrame(allocated_calc_instances)
        # expand metadata to columns
        df_meta = pd.DataFrame(df.pop('metadata').values.tolist(), columns=['a', 'b', 'expression'])
        df = df.join(df_meta)
        # compute results
        df['results']=df.apply(lambda r:calculate(r['a'],r['b'],r['expression']),axis=1)
        return df.values.tolist()

    @task
    def report_results(calc_results: List[List[Any]]):
        for calc in calc_results:
            print(f"Calculation {calc[0]}, with sort value {calc[1]}, consisting of expression {calc[4]} with inputs a={calc[2]} and b={calc[3]} has result {calc[5]}")

    # Main flow
    allocated = allocate_workers()
    calc_results=execute_calculations.expand(allocated_calc_instances=allocated)
    report_results.expand(calc_results=calc_results)




