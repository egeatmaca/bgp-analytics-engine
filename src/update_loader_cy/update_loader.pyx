from pybgpstream import BGPStream, BGPElem
import numpy as np
cimport numpy as cnp
import multiprocessing as mp
from cpython.datetime cimport datetime, timedelta
from time import time
import os
import csv


cdef class UpdateWriter:
    def __init__(self, file_name: str, data_dir: str = './data', n_rows: int = 1000000) -> None:
        self.header = ['record_type', 'type', 'time', 'project', 'collector', 
                       'router', 'router_ip', 'peer_asn', 'peer_address', 
                       'prefix', 'next_hop', 'as_path', 'communities']
        cdef list header
        cdef bint header_written
        cdef int row_iter
        cdef cnp.ndarray[object, ndim=2] rows
        cdef str file_path


        self.header_written = False
        self.n_rows = n_rows
        self.row_iter = 0
        self.rows = self.empty_rows()
        self.file_path = os.path.join(data_dir, file_name)
        os.makedirs(data_dir, exist_ok=True)

    cdef cnp.ndarray[object, ndim=2] empty_rows(self):
        cdef cnp.ndarray[object, ndim=1] empty_arr
        empty_arr = np.empty((self.n_rows, len(self.header)), dtype=object)
        return empty_arr

    cdef bint buffer_full(self):
        return self.row_iter >= self.n_rows

    cdef bint buffer_empty(self):
        return self.row_iter == 0

    cpdef void add_to_buffer(self, object update):
        cdef cnp.ndarray[object, ndim=1] update_row
        cdef str communities

        if "communities" in update.fields:
            communities = ' '.join(update.fields["communities"])
        else:
            communities = None

        update_row = np.array([
            update.record_type, update.type, update.time, update.project, update.collector,
            update.router, update.router_ip, update.peer_asn, update.peer_address,
            update._maybe_field("prefix"), update._maybe_field("next-hop"),
            update._maybe_field("as-path"), communities], 
            dtype=object
        )

        self.rows[self.row_iter] = update_row
        self.row_iter += 1

    cpdef void write_buffer(self):
        with open(self.file_path, "a") as f:
            writer = csv.writer(f)

            if not self.header_written:
                writer.writerow(self.header)
                self.header_written = True

            writer.writerows(self.rows[:self.row_iter])

        print(f'{self.row_iter} rows inserted!')

        # Reset the buffer after writing
        self.rows = self.empty_rows()
        self.row_iter = 0


cdef class UpdateLoader:
    IN_TIME_FMT: str = '%Y-%m-%d %H:%M:%S'
    OUT_TIME_FMT: str = '%Y-%m-%d %H:%M:%S.%f'

    cdef list collectors
    cdef datetime from_time, until_time
    cdef str filter_
    cdef int n_proc

    def __init__(self, collectors: list[str], from_time: str, until_time: str,
                 filter_: str | None = None, n_proc: int | None = None) -> None:
        self.collectors = collectors
        self.from_time = datetime.strptime(from_time, self.IN_TIME_FMT)
        self.until_time = datetime.strptime(until_time, self.IN_TIME_FMT)
        self.filter_ = filter_
        self.n_proc = n_proc if n_proc else mp.cpu_count()

    def split_time_ranges(self) -> list[tuple[datetime, datetime]]:
        time_delta = self.until_time - self.from_time
        range_seconds = time_delta.total_seconds() // self.n_proc
        range_delta = timedelta(seconds=range_seconds)
        microsecond_delta = timedelta(microseconds=1)

        time_ranges: list[tuple[datetime, datetime]] = []
        
        for i in range(self.n_proc):
            start_timestamp = self.from_time + i * range_delta
            
            if i < self.n_proc - 1:
                end_timestamp = start_timestamp + range_delta - microsecond_delta
            else:
                end_timestamp = self.until_time - microsecond_delta
            
            time_ranges.append((start_timestamp, end_timestamp))

        return time_ranges

    def load_update_range(self, from_time: datetime, until_time: datetime) -> None:
        from_time_str: str = from_time.strftime(self.OUT_TIME_FMT)
        until_time_str: str = until_time.strftime(self.OUT_TIME_FMT)

        stream = BGPStream(
            from_time=from_time_str,
            until_time=until_time_str,
            collectors=self.collectors,
            filter=self.filter_,
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

    def load_updates(self) -> None:
        started_at = datetime.now()

        time_ranges = self.split_time_ranges()

        print(f'Reading Updates for Collectors {self.collectors}')
        
        ctx = mp.get_context('spawn')
        
        with ctx.Pool(self.n_proc) as pool:
            pool.starmap(self.load_update_range, time_ranges)

        finished_at = datetime.now()
        
        print(f'Started At: {started_at}', f'Finished At: {finished_at}')

