import pybgpstream
import multiprocessing as mp
import os
import csv
from cpython.datetime cimport datetime, timedelta


cdef class UpdateWriter:
    cdef:
        tuple[
            str, str, str, str, 
            str, str, str, str, 
            str, str, str, str, str
        ] fields

        list[tuple[
            str, str, float, str, 
            str, str, str, int, 
            str, str, str, str, str
        ]] rows

        int n_rows
        int row_iter

        str file_path
        int write_iter

    
    def __init__(self, 
                 str file_name, 
                 str data_dir = './data',
                 int n_rows = 100000):
        self.fields = ('record_type', 'type', 'time', 'project', 'collector', 
                       'router', 'router_ip', 'peer_asn', 'peer_address', 
                       'prefix', 'next_hop', 'as_path', 'communities')
        self.n_rows = n_rows
        self.rows = self.n_rows * [None]
        self.row_iter = 0
        self.write_iter = 0
        self.file_path = os.path.join(data_dir, file_name)
        os.makedirs(data_dir, exist_ok=True)

    cpdef bint buffer_full(self):
        return self.row_iter >= self.n_rows

    cpdef bint buffer_empty(self):
        return self.row_iter == 0
    
    cpdef void add_to_buffer(self, update):
        cdef tuple[
            str, str, float, str, 
            str, str, str, int, 
            str, str, str, str, str
        ] update_row
        cdef set[str] communities_set
        cdef str community
        cdef str communities
        
        if "communities" in update.fields:
            communities_set = update.fields["communities"]
            communities = ' '.join(communities_set)
        else:
            communities = None
            
        update_row = (update.record_type, update.type, update.time, update.project, update.collector, update.router, update.router_ip,
                      update.peer_asn, update.peer_address, update._maybe_field("prefix"), update._maybe_field("next-hop"), 
                      update._maybe_field("as-path"), communities)
       
        self.rows[self.row_iter] = update_row
        self.row_iter += 1
    
    cpdef void write_buffer(self):
        with open(self.file_path, "a") as f:
            writer = csv.writer(f)
            
            if self.write_iter == 0:
                writer.writerow(self.fields)
            
            writer.writerows(self.rows[:self.row_iter])

        print(f'{self.row_iter} rows inserted!')
       
        self.rows = self.n_rows * [None]
        self.row_iter = 0
        self.write_iter += 1


cdef class UpdateLoader:
    cdef:
        list collectors
        str from_time
        str until_time
        str filter_
        int n_proc
        str IN_TIME_FMT
        str OUT_TIME_FMT

    def __init__(self, 
                 list collectors, 
                 str from_time, 
                 str until_time,
                 str filter_ = None,
                 int n_proc = 0):
        self.IN_TIME_FMT = '%Y-%m-%d %H:%M:%S'
        self.OUT_TIME_FMT = '%Y-%m-%d %H:%M:%S.%f'
        
        self.collectors = collectors
        self.from_time = from_time
        self.until_time = until_time
        self.filter_ = filter_
        self.n_proc = n_proc if n_proc else mp.cpu_count()
        
    cpdef list[tuple[str, str]] split_time_ranges(self):
        cdef:
            datetime from_time = datetime.strptime(self.from_time, self.IN_TIME_FMT)
            datetime until_time = datetime.strptime(self.until_time, self.IN_TIME_FMT)

            timedelta time_delta = until_time - from_time
            double range_seconds = time_delta.total_seconds() / self.n_proc
            timedelta range_delta = timedelta(seconds=range_seconds)
            timedelta microsecond_delta = timedelta(microseconds=1)

            str start_timestamp, end_timestamp
            list[tuple[str, str]] time_ranges = [] 
            int i

        for i in range(self.n_proc):
            start_timestamp = (
                    from_time + range_delta * i
            ).strftime(self.OUT_TIME_FMT)

            if i < self.n_proc - 1:
                end_timestamp = (
                        from_time + range_delta * (i+1) - microsecond_delta
                ).strftime(self.OUT_TIME_FMT)
            else:
                end_timestamp = (
                        until_time - microsecond_delta
                ).strftime(self.OUT_TIME_FMT)

            
            time_range = (start_timestamp, end_timestamp)
            time_ranges.append(time_range)

        return time_ranges

    def load_update_range(self, str from_time, str until_time):
        cdef:
            str file_name = f'{from_time} - {until_time}.csv'
            UpdateWriter update_writer
            object stream
            object item
        
        # Create BGP stream
        stream = pybgpstream.BGPStream(
            from_time=from_time, until_time=until_time,
            collectors=self.collectors,
            filter=self.filter_,
            record_type="updates"
        )

        update_writer = UpdateWriter(file_name)
 
        # Process stream items with efficient buffering
        for item in stream:
            update_writer.add_to_buffer(item)
            if update_writer.buffer_full():
                update_writer.write_buffer()

        # Flush remaining items
        if not update_writer.buffer_empty():
            update_writer.write_buffer()

        print(f'Updates read from {from_time} until {until_time}!') 

    def load_updates(self):
        cdef:
            datetime started_at = datetime.now()
            datetime finished_at
            timedelta time_delta
            list[tuple[str, str]] time_ranges
            str from_time, until_time
            object ctx
            object pool
        
        time_ranges = self.split_time_ranges() 

        print(f'Reading Updates for Collectors {self.collectors}')
        print(f'Proc Splits {self.from_time} - {self.until_time}:')
        for from_time, until_time in time_ranges:
            print('From:', from_time, '|', 'Until:', until_time)

        ctx = mp.get_context('spawn')
        with ctx.Pool(self.n_proc) as pool:
            pool.starmap(self.load_update_range, time_ranges)
            
        finished_at = datetime.now()
        time_delta = finished_at - started_at
        print(f'Started At: {started_at}', f'Finished At: {finished_at}')
        print(f'Total Time: {time_delta}')


