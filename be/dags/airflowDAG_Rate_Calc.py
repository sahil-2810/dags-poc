import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from be.brompton.WorkerAllocator import allocate
from be.brompton.WorkerAllocator import AllocateCandidate
from be.brompton.RateBackfill import *

# Schedule every n minutes
with DAG(
        dag_id="rate_calc_etl",
        schedule_interval="*/6 * * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["rate"],
        max_active_runs=1
) as dag:

    @task()
    def allocate_workers(max_workers=10, max_items_per_worker=120) -> List[list]:  # [[AllocateCandidate]]
        """
        #### Extract task
        """
        # TODO: track state of rate calc in ohter dags hosts
        # Get list of measurements needing rate calculation  (type is Volume description ends in count)
        hook = PostgresHook('datalogger_postgres')
        flow_ids_by_vol_id = get_ids_for_rate_tags(hook)

        # Get gaps
        gaps_by_id=find_most_recent_rate_gap(conn, flow_ids_by_vol_id)

        # Allocate them by total size of gaps ascending
        candidates=[]
        for id,flow_gap in gaps_by_id.items():
            if('end' in flow_gap['gap'] and flow_gap['gap']['end']>0): # gaps with no end means that volume has never been stored
                candidate=AllocateCandidate(id,flow_gap['gap']['end']-flow_gap['gap']['start'],gaps_by_id[id])
                candidates.append(candidate)
        allocated=allocate(candidates, max_workers=max_workers, max_items_per_worker=max_items_per_worker)
        return allocated


    @task()
    def fill_gaps(gaps_list_for_worker:List[AllocateCandidate]):
        print("list inside fill_gaps ",gaps_list_for_worker)
        fill_rate_gaps(conn,gaps_list_for_worker)

    # Main flow
    tdsb_host_parts=Variable.get("tsdb_host").split(":")
    conn=redis.Redis(tdsb_host_parts[0],int(tdsb_host_parts[1]))
    latest_gaps = allocate_workers()
    fill_gaps.expand(gaps_list_for_worker=latest_gaps)
