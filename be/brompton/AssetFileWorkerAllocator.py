from typing import List,Dict
from paramiko import SFTPAttributes
import logging

def allocate(extracted:Dict[str,List[SFTPAttributes]],max_workers=10, max_files_per_worker=120, max_files_per_asset=10) -> List[list]:  # [[{asset->filemanes}]]
    # sort by mod time and only keep filename
    print(f"FileWorkerAllocator called with maxworkers={max_workers}, max_files_per_worker={max_files_per_worker}, max_files_per_asset={max_files_per_asset}")
    length_of_values = sum([len(value) for _, value in extracted.items()])
    print(f"FileWorkerAllocator received {len(extracted)} asset keys, {length_of_values} files")
    temp = {}
    for asset, files in extracted.items():
        files.sort(key=lambda f: f.st_mtime)
        temp[asset] = [f.filename for f in files]
    filtered = temp

    # to be optimal (most assets served per dag run), process in order of asset file counts ascending
    filtered = dict(sorted(filtered.items(), key=lambda item: len(item[1])))

    total_files=sum([len(files) for k,files in filtered.items()])

    asset_files = []  # the whole enchilada
    total_cnt = 0
    worker_cnt = 0
    file_cnt = 0  # count of files per worker
    worker_files=[]
    target_files=min(total_files,(max_workers * max_files_per_worker))
    print(f"target files {target_files}")
    while(total_cnt<target_files):
        for asset, files in filtered.items():
            if(files):
                asset_file_cnt = len(files)
                print(f"processing {asset} with a remaining count of {asset_file_cnt} in worker {len(asset_files)}")
                allocation = min(asset_file_cnt, max_files_per_asset)
                worker_asset_row = {"asset": asset, "files": []}
                worker_files.append(worker_asset_row)
                for idx,file in enumerate(files[0:allocation]):
                    worker_asset_row['files'].append(file)
                    file_cnt += 1
                    total_cnt += 1
                    if (file_cnt >= max_files_per_worker or total_cnt >= target_files):  # worker full or workers exhausted
                        asset_files.append(worker_files)
                        worker_asset_row = {"asset": asset, "files": []}
                        worker_files = [worker_asset_row] if total_cnt<target_files and idx<(allocation-1) else []
                        worker_cnt+=1
                        file_cnt = 0
                        if(total_cnt>=target_files):
                           break
                if (total_cnt >= target_files):
                    break
                # track files left to process
                filtered[asset] = files[allocation:]
            # else:
            #      logger.debug(f"skipping asset {asset} which has no remaining files")
    if(worker_cnt<max_workers and worker_files):
        asset_files.append(worker_files)
    # print results
    print(f"allocated {len(asset_files)} workers")
    for idx,worker in enumerate(asset_files):
        total_files=0
        for row in worker:
            file_cnt=len(row["files"])
            if(not(file_cnt)):
                print(f"asset {row['asset']} allocated in worker {idx} with zero files")
            total_files+=file_cnt
        print(f"worker {idx} has {total_files} files")
    return asset_files
