import os
import csv


class UpdateWriter:
    def __init__(self, 
                 file_name: str, 
                 data_dir: str = './data',
                 max_buffer=100_000) -> None:
        self.file_path = os.path.join(data_dir, file_name)
        self.fields = ['record_type', 'type', 'time', 'project', 'collector', 
                       'router', 'router_ip', 'peer_asn', 'peer_address', 
                       'prefix', 'next_hop', 'as_path', 'communities']
        self.rows = []
        self.row_counter = 0
        self.max_buffer = max_buffer
        os.makedirs(data_dir, exist_ok=True)

    def buffer_full(self) -> bool:
        return self.row_counter >= self.max_buffer

    def buffer_empty(self) -> bool:
        return self.row_counter == 0
    
    def add_to_buffer(self, update):
        communities = ' '.join(update.fields["communities"]) if "communities" in update.fields else None,
        update_row = [update.record_type, update.type, update.time, update.project, update.collector, update.router, update.router_ip,
                      update.peer_asn, update.peer_address, update._maybe_field("prefix"), update._maybe_field("next-hop"), 
                      update._maybe_field("as-path"), communities]
        
        self.rows.append(update_row)
        self.row_counter += 1

    def write_buffer(self):
        data = [self.fields] + self.rows
        with open(self.file_path, "a") as f:
            writer = csv.writer(f)
            writer.writerows(data)

        print(f'{self.row_counter} rows inserted!')
        
        self.rows = []
        self.row_counter = 0


