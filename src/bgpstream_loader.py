from pybgpstream import BGPStream, BGPElem
import numpy as np
import multiprocessing as mp
import datetime as dt
from time import time
import os
import csv


class BGPStreamLoader:
    IN_TIME_FMT = '%Y-%m-%d %H:%M:%S'
    OUT_TIME_FMT = '%Y-%m-%d %H:%M:%S.%f'

    def __init__(self, 
                 collectors: list[str], 
                 from_time: str, 
                 until_time: str,
                 filter_: str|None = None,
                 n_proc: int|None = None,
                 data_dir: str = 'data') -> None:
        self.collectors = collectors
        self.from_time = dt.datetime.strptime(from_time, self.IN_TIME_FMT)
        self.until_time = dt.datetime.strptime(until_time, self.IN_TIME_FMT)
        self.filter_ = filter_
        self.n_proc = n_proc if n_proc else mp.cpu_count() 
        self.data_dir = data_dir
        self.header = ['record_type', 'type', 'time', 'project', 'collector', 
                       'router', 'router_ip', 'peer_asn', 'peer_address', 
                       'prefix', 'next_hop', 'as_path', 'communities']

    def parse_update(self, update: BGPElem):
        if "communities" in update.fields: 
            communities = ' '.join(update.fields["communities"]) 
        else:
            communities = None

        update_parsed = [
            update.record_type, update.type, update.time, update.project, update.collector, update.router, update.router_ip,
            update.peer_asn, update.peer_address, update._maybe_field("prefix"), update._maybe_field("next-hop"), 
            update._maybe_field("as-path"), communities
        ]
        return update_parsed

    def write_updates(self, from_time: dt.datetime, until_time: dt.datetime, file_path: str) -> None:
        stream = BGPStream(
            from_time = from_time.strftime(self.OUT_TIME_FMT),
            until_time = until_time.strftime(self.OUT_TIME_FMT),
            collectors=self.collectors,
            filter=self.filter_,
            record_type="updates"
        )

        with open(file_path, "a") as f:
            writer = csv.writer(f)
            print('WRITING HEADER!')
            writer.writerow(self.header)
            for update in stream:
                update_parsed = self.parse_update(update)
                writer.writerow(update_parsed)
                
        print(f'Updates read from {from_time} until {until_time}!') 

    def split_time_ranges(self) -> list[tuple[dt.datetime, dt.datetime]]:
        time_delta = self.until_time - self.from_time
        range_seconds = time_delta.total_seconds() // self.n_proc
        range_delta = dt.timedelta(seconds=range_seconds)
        microsecond_delta = dt.timedelta(microseconds=1)

        time_ranges = []
        for i in range(self.n_proc):
            start_timestamp = self.from_time + i * range_delta
            if i < self.n_proc - 1:
                end_timestamp = self.from_time + (i+1) * range_delta - microsecond_delta
            else:
                end_timestamp = self.until_time - microsecond_delta
            
            time_range = (start_timestamp, end_timestamp)
            time_ranges.append(time_range)
        
        return time_ranges
    
    def load_updates(self) -> None:
        started_at = dt.datetime.now()
         
        os.makedirs(self.data_dir, exist_ok=True)
        time_ranges = self.split_time_ranges() 
        
        print(f'Reading Updates for Collectors {self.collectors}')
        print(f'Proc Splits {self.from_time} - {self.until_time}:')
        for from_time, until_time in time_ranges:
            print('From:', from_time, '|', 'Until:', until_time)
        
        args = [(*tr, os.path.join(self.data_dir, f'{i}.csv')) 
                for i, tr in enumerate(time_ranges)]
        with mp.Pool(self.n_proc) as pool:
            pool.starmap(self.write_updates, args)

        finished_at = dt.datetime.now()
        time_delta = finished_at - started_at

        print(f'Started At: {started_at}', f'Finished At: {finished_at}')
        print(f'Total Time: {time_delta}')


