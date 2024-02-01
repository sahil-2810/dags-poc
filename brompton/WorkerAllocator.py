import random
import logging
from typing import List, Union, Any, Dict, OrderedDict
from datetime import datetime
from collections import namedtuple

# Marshall to known type
AllocateCandidate = namedtuple("AllocateCandidate", "id measure metadata")


def allocate(items: List[AllocateCandidate], max_workers=10, max_items_per_worker=120) \
        -> List[List[Union[
            Dict[str, Any], OrderedDict[str, Any]]]]:  # [[AllocateCandidate as OrderedDict for the benefit of airflow]]
    logger = logging.getLogger("WorkerAllocator")
    items.sort(key=lambda i: i.measure)  # prioritize by measure ascending
    total_items = len(items)
    total_cnt = 0
    worker_cnt = 0
    allocated_items = []
    target_items = min(total_items, (max_workers * max_items_per_worker))
    logger.debug(f"target items {target_items}")
    start = 0
    while (total_cnt < target_items and worker_cnt < max_workers):
        end = min(start + max_items_per_worker, total_items)
        worker_items = items[start:end]
        allocated_items.append([item._asdict() for item in worker_items])  # convert to dictionary for airflow's benefit
        total_cnt += len(worker_items)
        worker_cnt += 1
        start = end
    return allocated_items


if __name__ == '__main__':
    def generate_test_items(n: int, divisor: int):
        if (n < 2):
            n = 2
        ret = []
        if (not (n // divisor)):
            divisor = n + 1
        offset = n // divisor
        total = offset + round(n * (divisor - 1) * random.random() / divisor)
        for i in range(total):
            item = AllocateCandidate(f"item{i}", datetime.now().timestamp() - 10 * random.random(),
                                     {"meta1": random.randint(0, n)})
            ret.append(item)
        return ret


    n = 200
    items = generate_test_items(n, 4)
    print(f"INPUT length {n}")
    for item in items:
        print(item)
    allocated = allocate(items)
    for i, worker in enumerate(allocated):
        print(f"WORKER {i}:")
        for item in worker:
            print(item)
