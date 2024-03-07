import redis
from typing import List,Tuple,Any,Dict
import sys

def log_result(tvs, result)->bool:  # Returns true is result is good
    if (isinstance(result, Exception)):
        # ignore special cases, mark file as ingested anyways
        e_msg = str(result)
        if ('update is not supported' in e_msg or
                'older than retention' in e_msg or
                'equal or smaller' in e_msg):
            return True
        else:
            print(f"  Redis TS or Stream {tvs[0]} insert error:\n", result, file=sys.stderr)
            return False
    else:
        #     print(result)
        return True

# TODO: may need to limit pipe size
def bulk_store_in_tsdb(conn:redis.Redis,data:Dict[str,List[Tuple[int,int,Any]]]):  # 'STORE_TYPE'->[(key,ts,val)]}
    ts_ktv=[]
    with conn.pipeline(transaction=False) as pipe:
        for store_type,ktvs in data.items():
            if(store_type == 'STREAM'):
                for ktv in ktvs:
                    stream_params = [ktv[0], {"value": str(ktv[2])}, f"{ktv[1]}-0"]
                    pipe.xadd(*stream_params)
            else:
                for ktv in ktvs:
                    ts_params = (ktv[0], ktv[1], ktv[2])
                    ts_ktv.append(ts_params)
            pipe.ts().madd(ts_ktv)
            # Send to redis
            results = pipe.execute(raise_on_error=False)
            good_results = 0
            for tv, result in zip(ktvs, results):
                if (isinstance(result, list)):
                    for tv, res in zip(ktvs, result):
                        if (log_result(tv, res)):
                            good_results += 1
                else:
                    if (log_result(tv, result)):
                        good_results += 1
            print(f"{good_results} TSDB calculation entries stored")

def store_in_tsdb(conn:redis.Redis,tvs:List[Tuple[str,int,Any,str]]):  # id,ts,val,data_type
    is_stream={}
    ts_ktv=[]
    with conn.pipeline(transaction=False) as pipe:
        for tv in tvs:
            if(not(tv[0] in is_stream)):
                is_stream[tv[0]]=True if tv[3] in ('STRING','BOOLEAN') else False
            if (is_stream[tv[0]]):
                stream_params = [tv[0], {"value": str(tv[2])}, f"{tv[1]}-0"]
                pipe.xadd(*stream_params)
            else:
                ts_params = (tv[0], tv[1], tv[2])
                ts_ktv.append(ts_params)
        pipe.ts().madd(ts_ktv)
        # Send to redis
        results=pipe.execute(raise_on_error=False)
        good_results=0
        for tv,result in zip(tvs,results):
            if(isinstance(result,list)):
                for tv,res in zip(tvs,result):
                    if(log_result(tv, res)):
                        good_results+=1
            else:
                if(log_result(tv, result)):
                    good_results += 1
        print(f"{good_results} TSDB entries stored")
