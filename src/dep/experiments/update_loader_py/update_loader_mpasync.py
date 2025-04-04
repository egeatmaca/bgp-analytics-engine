from pybgpstream import BGPStream, BGPElem
import numpy as np
# import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import asyncio
import datetime as dt
from time import time
import os
import csv


class UpdateWriter:
    def __init__(self, 
                 file_name: str, 
                 data_dir: str = './data',
                 n_rows: int = 100_000) -> None:
        self.header = ['record_type', 'type', 'time', 'project', 'collector', 
                       'router', 'router_ip', 'peer_asn', 'peer_address', 
                       'prefix', 'next_hop', 'as_path', 'communities']
        self.header_written = False
        self.n_rows = n_rows
        self.row_iter = 0
        self.rows = self.empty_rows()
        self.file_path = os.path.join(data_dir, file_name)
        os.makedirs(data_dir, exist_ok=True)
    
    def empty_rows(self) -> np.ndarray:
        return np.empty((self.n_rows, len(self.header)), dtype=object)
    
    def write_buffer(self) -> None:
        if self.row_iter == 0:
            return

        with open(self.file_path, "a") as f:
            writer = csv.writer(f)

            if not self.header_written:
                writer.writerow(self.header)
                self.header_written = True

            writer.writerows(self.rows[:self.row_iter])

        print(f'{self.row_iter} rows inserted!')
        
        self.rows = self.empty_rows()
        self.row_iter = 0
   
    def add_to_buffer(self, update: BGPElem) -> None:
        if "communities" in update.fields: 
            communities = ' '.join(update.fields["communities"]) 
        else:
            communities = None

        update_row = np.array([
            update.record_type, update.type, update.time, update.project, update.collector, update.router, update.router_ip,
            update.peer_asn, update.peer_address, update._maybe_field("prefix"), update._maybe_field("next-hop"), 
            update._maybe_field("as-path"), communities
        ])
        
        self.rows[self.row_iter] = update_row
        self.row_iter += 1

        if self.row_iter >= self.n_rows:
            self.write_buffer()


IN_TIME_FMT = '%Y-%m-%d %H:%M:%S'
OUT_TIME_FMT = '%Y-%m-%d %H:%M:%S.%f'

def split_time_ranges(from_time: dt.datetime, 
                      until_time: dt.datetime, 
                      n_splits: int) -> list[tuple[dt.datetime, dt.datetime]]:
    time_delta = until_time - from_time
    range_seconds = time_delta.total_seconds() // n_splits 
    range_delta = dt.timedelta(seconds=range_seconds)
    microsecond_delta = dt.timedelta(microseconds=1)

    time_ranges = []
    for i in range(n_splits):
        start_timestamp = from_time + i * range_delta
        if i < n_splits - 1:
            end_timestamp = from_time + (i+1) * range_delta - microsecond_delta
        else:
            end_timestamp = until_time - microsecond_delta
         
        time_range = (start_timestamp, end_timestamp)
        time_ranges.append(time_range)
     
    return time_ranges
   
def load_updates_task(collectors: list[str],
                      from_time: dt.datetime, 
                      until_time: dt.datetime,
                      filter_: str = None) -> None:
    from_time_str = from_time.strftime(OUT_TIME_FMT)
    until_time_str = until_time.strftime(OUT_TIME_FMT)
    update_file = f'{from_time_str} - {until_time_str}.csv'

    # Read from BGPStream and write with UpdateWriter
    stream = BGPStream(
        from_time = from_time_str,
        until_time = until_time_str,
        collectors=collectors,
        filter=filter_,
        record_type="updates"
    )
    writer = UpdateWriter(update_file)

    for item in stream:
        writer.add_to_buffer(item)
    writer.write_buffer()

    print(f'Updates read from {from_time} until {until_time}!') 

async def load_updates(collectors: list[str],
                       from_time: dt.datetime, 
                       until_time: dt.datetime,
                       filter_: str = None,
                       n_tasks: int = 8) -> None:
    started_at = dt.datetime.now()
    print(f'Reading Updates for Collectors {collectors}')

    # Parse input
    from_time = dt.datetime.strptime(from_time, IN_TIME_FMT)
    until_time = dt.datetime.strptime(until_time, IN_TIME_FMT)

    # Split time ranges
    time_ranges = split_time_ranges(from_time, until_time, n_tasks)  

    print(f'Splits {from_time} - {until_time}:')
    for from_time, until_time in time_ranges:
        print('From:', from_time, '|', 'Until:', until_time)
    
    # Run async tasks inside process pool
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor() as executor:
        tasks = []
        for from_time, until_time in time_ranges:
            args =(collectors, from_time, until_time, filter_)
            task = loop.run_in_executor(executor, load_updates_task, *args) 
            tasks.append(task)
        
        asyncio.gather(*tasks)

    finished_at = dt.datetime.now()
    time_delta = finished_at - started_at

    print(f'Started At: {started_at}', f'Finished At: {finished_at}')
    print(f'Total Time: {time_delta}')

def run_load_updates(*args, **kwargs):
    asyncio.run(load_updates(*args, **kwargs))
