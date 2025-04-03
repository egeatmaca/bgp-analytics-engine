import pybgpstream
import multiprocessing as mp
import datetime as dt
from time import time
import os
import csv


class UpdateWriter:
    def __init__(self, 
                 file_name: str, 
                 data_dir: str = './data',
                 max_buffer: int = 100_000) -> None:
        self.file_path = os.path.join(data_dir, file_name)
        self.fields = ['record_type', 'type', 'time', 'project', 'collector', 
                       'router', 'router_ip', 'peer_asn', 'peer_address', 
                       'prefix', 'next_hop', 'as_path', 'communities']
        self.rows: list[list] = []
        self.row_counter = 0
        self.max_buffer = max_buffer
        os.makedirs(data_dir, exist_ok=True)

    def buffer_full(self) -> bool:
        return self.row_counter >= self.max_buffer

    def buffer_empty(self) -> bool:
        return self.row_counter == 0
    
    def add_to_buffer(self, update) -> None:
        communities = ' '.join(update.fields["communities"]) if "communities" in update.fields else None,
        update_row = [update.record_type, update.type, update.time, update.project, update.collector, update.router, update.router_ip,
                      update.peer_asn, update.peer_address, update._maybe_field("prefix"), update._maybe_field("next-hop"), 
                      update._maybe_field("as-path"), communities]
        
        self.rows.append(update_row)
        self.row_counter += 1

    def write_buffer(self) -> None:
        data = [self.fields] + self.rows
        with open(self.file_path, "a") as f:
            writer = csv.writer(f)
            writer.writerows(data)

        print(f'{self.row_counter} rows inserted!')
        
        self.rows = []
        self.row_counter = 0


class UpdateLoader:
    IN_TIME_FMT = '%Y-%m-%d %H:%M:%S'
    OUT_TIME_FMT = '%Y-%m-%d %H:%M:%S.%f'

    def __init__(self, 
                 collectors: list[str], 
                 from_time_str: str, 
                 until_time_str: str,
                 filter: str|None = None,
                 n_proc: int|None = None) -> None:
        self.collectors = collectors
        self.from_time = dt.datetime.strptime(from_time_str, self.IN_TIME_FMT)
        self.until_time = dt.datetime.strptime(until_time_str, self.IN_TIME_FMT)
        self.filter = filter
        self.n_proc = n_proc if n_proc else mp.cpu_count() 
        
    def split_time_ranges(self) -> list[tuple[dt.datetime, dt.datetime]]:
        time_delta = self.until_time - self.from_time
        range_seconds = time_delta.total_seconds() // self.n_proc
        range_delta = dt.timedelta(seconds=range_seconds)
        microsecond_delta = dt.timedelta(microseconds=1)

        time_ranges: list[tuple[dt.datetime, dt.datetime]] = []
        for i in range(self.n_proc):
            start_timestamp = self.from_time + i * range_delta
            if i < self.n_proc - 1:
                end_timestamp = self.from_time + (i+1) * range_delta - microsecond_delta
            else:
                end_timestamp = self.until_time - microsecond_delta
            
            time_range = (start_timestamp, end_timestamp)
            time_ranges.append(time_range)

        return time_ranges

    def load_update_range(self, from_time: dt.datetime, until_time: dt.datetime):
        from_time_str: str = from_time.strftime(self.OUT_TIME_FMT)
        until_time_str: str = until_time.strftime(self.OUT_TIME_FMT)
        
        stream = pybgpstream.BGPStream(
            from_time=from_time_str, until_time=until_time_str,
            collectors=self.collectors,
            filter=self.filter,
            record_type="updates"
        )

        file_name = f'{from_time} - {until_time}.csv'
        update_writer = UpdateWriter(file_name)
 
        for item in stream:
            update_writer.add_to_buffer(item)
            if update_writer.buffer_full():
                update_writer.write_buffer()

        if not update_writer.buffer_empty():
            update_writer.write_buffer()

        print(f'Updates read from {from_time} until {until_time}!') 

    def load_updates(self):
        started_at = dt.datetime.now()

        time_ranges = self.split_time_ranges() 
        
        print(f'Reading Updates for Collectors {self.collectors}')
        print(f'Proc Splits {self.from_time} - {self.until_time}:')
        for time_range in time_ranges:
            print('Start:', time_range[0], '|', 'End:', time_range[1])

        ctx = mp.get_context('spawn')
        processes = []
        for from_time, until_time in time_ranges:
            p = ctx.Process(target=self.load_update_range, 
                            args=(from_time, until_time))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

        finished_at = dt.datetime.now()
        time_delta = finished_at - started_at
        print(f'Started At: {started_at}', f'Finished At: {finished_at}')
        print(f'Total Time: {time_delta}')


