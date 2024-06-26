import sys
from typing import List, Dict
import redis
from redis.sentinel import Sentinel
from airflow import DAG
from airflow.decorators import task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# Redirect to local imports if not Cloud environment
try:
    from be.brompton.AssetFileWorkerAllocator import allocate
except:
    from brompton.AssetFileWorkerAllocator import allocate
try:
    from be.brompton.SymbolToIDMapping import get_maps_from_db
except:
    from brompton.SymbolToIDMapping import get_maps_from_db
try:
    from be.brompton.TSDBUtils import *
except:
    from brompton.TSDBUtils import *

import pendulum
import paramiko


def sftp_exists(sftp, path):
    try:
        sftp.stat(path)
        return True
    except FileNotFoundError:
        print("filePath ",path)
        return False
    except IOError:
        print("filePath ",path)
        return False

poll_period = int(Variable.get("sftp_poll"))
max_workers = int(Variable.get("max_workers"))
max_files_per_worker = int(Variable.get("max_files_per_worker"))
max_files_per_asset = int(Variable.get("max_files_per_asset"))

# Schedule every n minutes
with DAG(
        dag_id="sftp_etl_v2",
        schedule_interval=f"*/{poll_period} * * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["sftp"],
        max_active_runs=1
) as dag:
    @task()
    def allocate_workers(file_pattern, max_workers=max_workers, max_files_per_worker=max_files_per_worker,
                         max_files_per_asset=max_files_per_asset) -> List[list]:  # [[{asset->filemanes}]]
        """
        #### Extract task
        Get files in order of modification and pass them to downstream
        processes grouped by asset.
        """
        # TODO: track state of files in Datalogger DB for cluster processing
        # Read inprocess files

        import fnmatch

        filtered = {}

        hook = SFTPHook("staged_files_sftp")
        with hook.get_conn() as sftp:
            # Files grouped by asset
            for file in sftp.listdir_attr("upload"):
                if fnmatch.fnmatch(file.filename, file_pattern):
                    asset = file.filename.split("-")[1]
                    if (not (asset in filtered)):
                        filtered[asset] = []
                    filtered[asset].append(file)

        if (not (filtered)):
            return []

        # TODO: mark in process
        return allocate(filtered, max_workers=max_workers, max_files_per_worker=max_files_per_worker,
                        max_files_per_asset=max_files_per_asset)


    @task()
    def map_to_tags(asset_files: list, file_pattern: str) -> List[
        Dict]:  # [{"asset":asset,"data":[{"file":file,"data":data},...],...]
        """
        #### map to tags task
        For a set of assets (asset is dict key) gets a list of files to process.
        In each file, transform headers, replacing them with redis keys to use in loading:
        1. Get tagname corresponding to symbol
        2. Using asset id and tagname get key
        3. Output is list of dictionaries [{asset->{file->data with headers}]
        """
        maps = get_maps_from_db(file_pattern=file_pattern, assets=asset_files, hook=PostgresHook('datalogger_postgres'))

        # TODO: handle common connection
        file_blocks = []
        hook = SFTPHook("staged_files_sftp")
        with hook.get_conn() as sftp:
            for asset_file_block in asset_files:
                asset = asset_file_block["asset"]
                misconf = not (int(asset) in maps)
                asset_data = {"asset": asset, "data": []}
                print(f"Transforming files for asset {asset}")
                for file in asset_file_block["files"]:
                    if (misconf):
                        print(f"Missing configuration for asset id {asset} ignoring file {file}")
                        # Move to misconfig bucket
                        sftp.rename(f"upload/{file}", f"upload/misconfig/{file}")
                        continue
                    print(f"Transforming data for file {file}")
                    file_data = {"file": file, "data": []}
                    header = True
                    cols = []
                    skip_cols = []
                    with sftp.open(f"upload/{file}") as fin:
                        line = fin.readline()
                        while (line):
                            if (header):
                                header_cols = line[0:-1].split(",")
                                # TODO: replace header with corresponding key
                                for idx, col in enumerate(header_cols):
                                    if (col in maps[int(asset)]):
                                        cols.append(maps[int(asset)][col])
                                    elif (
                                            col == 'Time'):  # TODO: make this configurable in new table ETL_TIMESTAMPS similar by asset type
                                        cols.append(col)
                                    else:
                                        print(f"Key {col} not defined in mappings - skipping", file=sys.stderr)
                                        skip_cols.append(idx)
                                header = False
                            else:
                                vals = line[0:-1].split(",")
                                if (len(vals) == len(cols) + len(skip_cols)):  # skip imcomplete rows
                                    vals = [val for idx, val in enumerate(vals) if not (idx in skip_cols)]
                                    row = dict(zip(cols, vals))
                                    file_data["data"].append(row)
                            line = fin.readline()
                    asset_data["data"].append(file_data)
                file_blocks.append(asset_data)
        return file_blocks


    @task()
    def load(file_block: list) -> List[
        Dict]:  # [{"asset":asset,"dispositions":[{"file":file,"delete":True or False},...],..]
        """
        #### Load files to redis task
        For a set of assets (asset is dict key) load file data into redis
        """
        dispositions = []
        # Connect to redis for each file block
        print(f"Redis Python API version: {redis.__version__}")
        # TODO: Move to variables.  Also secure.
        if (tsdb_master):
            sentinel = Sentinel(sentinels=[(tsdb_host, tsdb_port),
                                           ], socket_timeout=10, sentinel_kwargs={'password': tsdb_pwd},
                                password=tsdb_pwd)
            # sentinel = Sentinel(sentinels=[('redis-service', 26379),
            #       ],socket_timeout=10,sentinel_kwargs={'password': 'test@123'},password='test@123')
            conn = sentinel.master_for(tsdb_master)
        else:
            conn = redis.Redis(tsdb_host, tsdb_port, password=tsdb_pwd)
        key_types = {}
        for asset_files in file_block:
            asset = asset_files['asset']
            file_data = asset_files['data']
            asset_dispositions = {"asset": asset, "dispositions": []}
            for f in file_data:
                file_keys=[]
                file = f['file']
                print(f"Loading file {file}")
                disposition = {"file": file, "processed": False, "errors": False}
                # Create a pipeline per file
                with conn.pipeline(transaction=False) as pipe:
                    for row in f["data"]:
                        time = int(row["Time"]) * 1000
                        for k, v in row.items():
                            if (k == 'Time'):
                                continue
                            if (not (k in key_types)):
                                key_types[k] = conn.type(k).decode('utf-8')
                            if (key_types[k] == 'TSDB-TYPE'):
                                ts_params = [k, time, float(v)]
                                # pipe.ts().add(*ts_params,duplicate_policy="LAST")
                                pipe.ts().add(*ts_params)
                            #                               print(f"TSDB Type {k}:{v}")
                            else:
                                stream_params = [k, {"value": str(v)}, f"{time}-0"]
                                pipe.xadd(*stream_params)
                                print(f"Not TSDB Type {k}:{v}")
                        file_keys.append(k)
                    # Send to redis entire file
                    # NOTE: Changed not to fail entire file but only items with problems
                    try:
                        results=pipe.execute(raise_on_error=False)
                        critical_errors = 0
                        for tsdb_key, result in zip(file_keys, results):
                            if not(log_result([tsdb_key], result)):
                                        critical_errors += 1
                        disposition["processed"] = True
                        if(critical_errors):
                            print(f"{critical_errors} TSDB insert errors detected in file {file}")
                            disposition["errors"] = True
                    except Exception as e:
                        disposition["errors"] = True
                        print(f"     {file}:\n", e, file=sys.stderr)
                    # try:
                    #     pipe.execute()
                    #     disposition["processed"] = True
                    # except Exception as e:
                    #     str_e = str(e)
                    #     # ignore special cases, mark file as ingested anyways
                    #     if ('update is not supported' in str_e or
                    #             'older than retention' in str_e or
                    #             'equal or smaller' in str_e):
                    #         disposition["processed"] = True
                    #     disposition["errors"] = True
                    #     print(f"     {file}:\n", e, file=sys.stderr)
                asset_dispositions["dispositions"].append(disposition)
            dispositions.append(asset_dispositions)
        return dispositions


    @task()
    def handle_files(dispositions: list):
        """
        #### Handle Files task
        For a set of assets (asset is dict key) gets a list of file dispositions
        """
        # TODO: handle common connection
        hook = SFTPHook("staged_files_sftp")
        with hook.get_conn() as sftp:
            # delete files that were loaded
            for asset_file_dispositions in dispositions:
                asset = asset_file_dispositions['asset']
                print(f"Handling dispositions of files for asset {asset}")
                for file_disposition in asset_file_dispositions['dispositions']:
                    file = file_disposition["file"]
                    duplicate = False
                    if (file_disposition["processed"] is True and file_disposition["errors"] is False):
                        print(f"Relocating processed file {file}")
                        processed_path = f"upload/processed/{file}"
                        if (not (sftp_exists(sftp, processed_path))):
                            sftp.rename(f"upload/{file}", processed_path)
                        else:
                            print(
                                f"ERROR: file {processed_path} already exists, this may contain duplicate timestamps!!! - moving file to errors directory path")
                            duplicate = True
                    if (duplicate or (file_disposition["processed"] is True and file_disposition["errors"] is True)):
                        if (not (duplicate)):
                            print(f"Errors in processed file {file}")
                        errors_path = f"upload/errors/{file}"
                        if (sftp_exists(sftp, errors_path)):
                            print(
                                f"ERROR: file {errors_path} already exists, this must contain duplicate timestamps!!!")
                        sftp.rename(f"upload/{file}", errors_path)


    # Main flow
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
    file_pattern = "AN*.csv"
    extracted = allocate_workers(file_pattern=file_pattern)
    transformed = map_to_tags.expand(asset_files=extracted, file_pattern=[file_pattern])
    file_dispositions = load.expand(file_block=transformed)
    handle_files.expand(dispositions=file_dispositions)