import pybgpstream
import multiprocessing as mp
import asyncio
import datetime as dt
from time import time
from table_buffer import UpdatesBuffer


class UpdateLoader:
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, 
                 collectors: list[str], 
                 from_time: str, 
                 until_time: str,
                 filter: str=None, 
                 n_tasks: int = 50) -> None:
        self.collectors = collectors
        self.from_time = dt.datetime.strptime(from_time, self.TIME_FORMAT)
        self.until_time = dt.datetime.strptime(until_time, self.TIME_FORMAT)
        self.filter = filter
        self.n_tasks = n_tasks
        
    def split_time_ranges(self, from_time, until_time, n_splits):
        time_delta = until_time - from_time
        range_seconds = time_delta.total_seconds() // n_splits
        range_delta = dt.timedelta(seconds=range_seconds)
        one_second_delta = dt.timedelta(seconds=1)

        time_ranges = []
        for i in range(n_splits):
            start_timestamp = from_time + i * range_delta
            if i < n_splits - 1:
                end_timestamp = from_time + (i+1) * range_delta - one_second_delta
            else:
                end_timestamp = until_time


            time_range = (start_timestamp, end_timestamp)
            time_ranges.append(time_range)

        return time_ranges

    async def load_updates_task(self, from_time, until_time):
        from_time = from_time.strftime(self.TIME_FORMAT)
        until_time = until_time.strftime(self.TIME_FORMAT)
        
        stream = pybgpstream.BGPStream(
            from_time=from_time, until_time=until_time,
            collectors=self.collectors,
            filter=self.filter,
            record_type="updates"
        )
        updates_buffer = UpdatesBuffer()
    
        for item in stream:
            updates_buffer.add_to_buffer(item)
            if updates_buffer.is_full():
                updates_buffer.insert_buffer()

        if not updates_buffer.is_empty():
            updates_buffer.insert_buffer()

        print(f'Updates read from {from_time} until {until_time}!') 

    async def load_updates_async(self):
        started_at = dt.datetime.now()

        UpdatesBuffer().check_create_table() 

        time_ranges = self.split_time_ranges(self.from_time, 
                                             self.until_time, 
                                             self.n_tasks)
        
        print(f'Reading Updates for Collectors {self.collectors}')
        print(f'Task Splits {self.from_time} - {self.until_time}:')
        for time_range in time_ranges:
            print('Start:', time_range[0], '|', 'End:', time_range[1])

        async with asyncio.TaskGroup() as tg:
            for from_time, until_time in time_ranges:
                coroutine = self.load_updates_task(from_time, until_time)
                task = tg.create_task(coroutine)

        finished_at = dt.datetime.now()
        time_delta = finished_at - started_at
        print(f'Started At: {started_at}', f'Finished At: {finished_at}')
        print(f'Total Time: {time_delta}')

    def load_updates(self):
        asyncio.run(self.load_updates_async())

