from pybgpstream import BGPStream, BGPElem
import numpy as np
import multiprocessing as mp
import datetime as dt
from time import time
import os
import csv


class BGPStreamWriter:
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

    def write_stream(self, stream: BGPStream) -> None:
        for update in stream:
            writer.add_to_buffer(update)

        writer.write_buffer()


class BGPStreamLoader:
    IN_TIME_FMT = '%Y-%m-%d %H:%M:%S'
    OUT_TIME_FMT = '%Y-%m-%d %H:%M:%S.%f'

    def __init__(self, 
                 collectors: list[str], 
                 from_time: str, 
                 until_time: str,
                 filter_: str|None = None,
                 n_proc: int|None = None) -> None:
        self.collectors = collectors
        self.from_time = dt.datetime.strptime(from_time, self.IN_TIME_FMT)
        self.until_time = dt.datetime.strptime(until_time, self.IN_TIME_FMT)
        self.filter_ = filter_
        self.n_proc = n_proc if n_proc else mp.cpu_count() 
       
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
    
    def load_update_range(self, from_time: dt.datetime, until_time: dt.datetime) -> None:
        from_time_str = from_time.strftime(self.OUT_TIME_FMT)
        until_time_str = until_time.strftime(self.OUT_TIME_FMT)
        update_file = f'{from_time_str} - {until_time_str}.csv'

        stream = BGPStream(
            from_time = from_time_str,
            until_time = until_time_str,
            collectors=self.collectors,
            filter=self.filter_,
            record_type="updates"
        )
        writer = BGPStreamWriter(update_file)
        writer.write_stream(stream)
        
        print(f'Updates read from {from_time} until {until_time}!') 

    def load_updates(self) -> None:
        started_at = dt.datetime.now()
        time_ranges = self.split_time_ranges() 
        
        print(f'Reading Updates for Collectors {self.collectors}')
        print(f'Proc Splits {self.from_time} - {self.until_time}:')
        for from_time, until_time in time_ranges:
            print('From:', from_time, '|', 'Until:', until_time)

        with mp.Pool(self.n_proc) as pool:
            pool.starmap(self.load_update_range, time_ranges)

        finished_at = dt.datetime.now()
        time_delta = finished_at - started_at

        print(f'Started At: {started_at}', f'Finished At: {finished_at}')
        print(f'Total Time: {time_delta}')


