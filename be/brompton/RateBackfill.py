import redis
import numpy as np
from typing import List,Dict,Union,Any,Tuple

# Get list of measurements needing rate calculation  (type is Volume description ends in count)
def get_ids_for_rate_tags(hook)->Dict[str,str]: # vol_key->rate_key
    qry = f"""select mv.id,coalesce(mf.id::TEXT, (mv.id::TEXT || '_rate')),mv.tag
        from (select * from v_measurements where tag like '%\\Volume'  
        and coalesce(description,'') not like '%demo%' and meas_type like '%count') mv 
        left join (select * from v_measurements where tag like '%\\Flow' and meas_type like '%calculated') mf
        on mf.cust_id=mv.cust_id
        and mf.parent_asset=mv.parent_asset and coalesce(mf.location,'*')=coalesce(mv.location,'*')"""
    f_ids={}
    cur = None
    try:
        # connect to db
        conn = hook.get_conn()
        print(f"Reading measurement id's needing rate calculation")
        cur = conn.cursor()
        cur.execute(qry)
        rows = cur.fetchall()
        for row in rows:
            f_ids[row[0]]=row[1]
    except Exception as e:
        raise(e)
    finally:
        if(cur):
            cur.close()
    return f_ids

def find_most_recent_rate_gap(conn:redis.Redis, redis_rate_keys:Dict[str, str]  # vol_key->rate_key
                              )->Dict[str,Dict[str,Union[str,Dict[str,Any]]]]:  # vol_key->{rate:rate_key,gap:{start:ts,end:ts}}
    latest_gap={}
    # read all info in a single transaction
    with conn.pipeline(transaction=False) as pipe:
        # raw data
        for raw_key,_ in redis_rate_keys.items():  # raw key is the volume key,
            pipe.ts().info(raw_key)
        results= pipe.execute(raise_on_error=False)
        # get gap end (most recent source timestamp)
        for raw_key, rate_key, result in zip(list(redis_rate_keys.keys()),list(redis_rate_keys.values()),results):
            latest_gap[raw_key] = {"rate":rate_key,"gap":{}}
            if (isinstance(result, redis.ResponseError)):
                print(f"ERROR: retrieving last timestamp from volume tag: {raw_key}: {result.args[0]}")
            else:
                latest_gap[raw_key]["gap"]["end"]=result.last_timestamp
        # get rate data to complete the picture
        for raw_key,gap in latest_gap.items():
            pipe.ts().info(gap['rate'])
        results = pipe.execute(raise_on_error=False)
        # get gap start (most recent rate timestamp)
        for raw_key,rate_key,result in zip(list(redis_rate_keys.keys()),list(redis_rate_keys.values()),results):
            if (isinstance(result, redis.ResponseError)):
                print(f"ERROR: retrieving last timestamp from rate:{rate_key} for volume: {raw_key}: {result.args[0]}")
            else:
                latest_gap[raw_key]['gap']["start"]=result.last_timestamp
        return latest_gap

# compute rate in seconds
def compute_rate_from_totalizer(raw_data,ts_is_ms=True):  # returns rate in seconds and time in ms for tsdb
    # compute rate using numpy
    results_array = np.array(raw_data)
    results_array[1:, 1] = (results_array[1:, 1] - results_array[:-1, 1]) /(results_array[1:, 0] - results_array[:-1, 0])
    if(ts_is_ms):
        results_array[1:,1]=1000*results_array[1:,1]
        results_array[1:,0]=results_array[1:,0]
    else:
        results_array[1:,0]=1000*results_array[1:,0]
    # set rate to zero if volume drops (negative rate) - totalizer was reset
    results_array[:,1][results_array[:, 1] < 0] = 0
    return results_array[1:]  # drop first result which has no prev value to compute rate

MAX_MSECS=5*24*60*60*1000 # maximum number of seconds to backfill per call
def back_fill_rate_from_totalizer(conn:redis.Redis, raw_key, rate_key, gap):
    # Convert gap to seconds
    tstart = gap['start']
    tend = gap['end']
    while (tstart < tend):
        # get raw values needed for calculation
        tblockend = min(tstart + MAX_MSECS, tend)
        # find source data (e.g. volume from which to compute rate
        raw_data = conn.ts().range(raw_key, tstart, tblockend)  # start is same as last end ensuring that value from prev block is used
        if (isinstance(raw_data, redis.ResponseError)):  # cannot compute rate
            print(
                f"ERROR: retrieving raw values for volume:{raw_key} between {tstart} and  {tblockend} timestamps: {raw_data.args[0]}")
        # protect from results of only 1 value (cannot compute rate)
        elif (len(raw_data) > 1):
            print(f"Filling gap for tag {rate_key} between {tstart} and {tblockend}")
            # update the values in a single transaction
            with conn.pipeline(transaction=False) as pipe:
                results_array = compute_rate_from_totalizer(raw_data)
                for vts in results_array:
                    pipe.ts().add(rate_key, int(vts[0]), vts[1])
                results = pipe.execute(raise_on_error=False)
                for result in results:
                    if (isinstance(result, redis.ResponseError)):
                        # TODO: log error here
                        print(f"Error inserting value {vts[1]} for rate:{rate_key} at {vts[0]} timestamp: {result.args[0]}")
        tstart = tblockend

# This only allows up to 180 days of backfilling
MAXRETENTION=180 * 24 * 60 * 60 * 1000
# Fill rate gags
def fill_rate_gaps(conn:redis.Redis,latest_gaps:List):
        for meter in latest_gaps:
            raw_key=meter['id']
            # Temporarily change retention
            rate_key=meter['metadata']['rate']
            try:
                info = conn.ts().info(rate_key)
            except Exception as e:
                if ("key does not exist" in str(e)):
                    print(f"Timeseries {id} does not exist - skipping rate update")
                    continue
                else:
                    raise e
            old_retention = info.retention_msecs
            conn.ts().alter(rate_key,retention_msecs=MAXRETENTION)
            # For each gap, loop through filling with calculated rate
            if(isinstance(meter['metadata']['gap'],list)): # if nore than one gap
                for gap in meter['metadata']['gap']:
                    back_fill_rate_from_totalizer(conn, raw_key, rate_key, gap)
            else:
                back_fill_rate_from_totalizer(conn, raw_key, rate_key, meter['metadata']['gap'])
            # Return retention back to original number
            conn.ts().alter(rate_key, retention_msecs=old_retention)