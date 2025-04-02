from sqlalchemy import create_engine, text
from abc import ABC, abstractmethod
import settings


class Field:
    def __init__(self, field_name: str, sql_dtype: str, **kwargs) -> None:
        self.field_name = field_name
        self.sql_dtype = sql_dtype

        for kw, arg in kwargs.items():
            setattr(self, kw, arg)

        if not hasattr(self, 'source_field'):
            self.source_field = field_name


class TableBuffer(ABC):
    def __init__(self, table_name: str, fields: list[Field], max_buffer=10000, db_url: str = None) -> None:
        self.table_name = table_name
        self.fields = fields
        self.field_names = self.serialize_fields()
        self.schema = self.serialize_schema()
        self.item_buffer = ''
        self.buffer_counter = 0
        self.max_buffer = max_buffer
        db_url = db_url if db_url else settings.DB_URL 
        self.engine = create_engine(db_url)
    
    def serialize_schema(self) -> str:
        serialized = ''
        for field in self.fields:
            serialized += f'{field.field_name} {field.sql_dtype},'
        serialized = f'({serialized[:-1]})'
        return serialized

    def serialize_fields(self) -> str:
        serialized = ''
        for field in self.fields:
            serialized += f'{field.field_name},'
        serialized = f'({serialized[:-1]})'
        return serialized

    @abstractmethod
    def serialize_item(self, item: object) -> str:
        pass

    def execute_sql(self, query): 
        query_lower = query.strip().lower()
        to_read = query_lower.startswith('select')
        to_commit = query_lower.startswith('create')
        to_commit = to_commit or query_lower.startswith('insert')
        to_commit = to_commit or query_lower.startswith('update')
        to_commit = to_commit or query_lower.startswith('delete')

        with self.engine.connect() as conn:
            results = conn.execute(text(query))
            if to_read:
                results = results.mappings().all()
            elif to_commit:
                conn.commit()
                results = True
            else:
                raise ValueError('Query type not supported!') 

        return results
        

    def table_exists(self):
        query = f'''
            SELECT EXISTS (
                SELECT 1 FROM 
                    pg_tables
                WHERE 
                    schemaname = 'public' AND 
                    tablename  = '{self.table_name}'
            );
        '''
        results = self.execute_sql(query)
        return results[0]['exists']

    def check_create_table(self):
        if not self.table_exists():
            query = f'CREATE TABLE {self.table_name}{self.schema};'
            results = self.execute_sql(query)
            print(f'Table {self.table_name} is created!')
            return True

        print(f'Table {self.table_name} already exists!')
        return False

    def add_to_buffer(self, item):
        item_serialized = self.serialize_item(item)
        if self.item_buffer != '':
            self.item_buffer += ',\n'
        self.item_buffer += item_serialized
        self.buffer_counter += 1

    def insert_buffer(self):
        query = f'''INSERT INTO {self.table_name} {self.field_names}
                    VALUES {self.item_buffer};'''
        results = self.execute_sql(query)
        
        if results:
            print(f'{self.buffer_counter} rows inserted!')
        
        self.item_buffer = ''
        self.buffer_counter = 0

        return results

    def is_full(self) -> bool:
        return self.buffer_counter >= self.max_buffer

    def is_empty(self) -> bool:
        return self.buffer_counter == 0


class UpdatesBuffer(TableBuffer):
    def __init__(self):
        table_name = 'updates' 
        fields = [
            Field('record_type', 'VARCHAR(21)'),
            Field('type', 'VARCHAR(32)'),
            Field('time', 'FLOAT'),
            Field('project', 'VARCHAR(32)'),
            Field('collector', 'VARCHAR(64)'),
            Field('router', 'VARCHAR(64)'),
            Field('router_ip', 'VARCHAR(64)'),
            Field('peer_asn', 'INT'),
            Field('peer_address', 'VARCHAR(64)'),
            Field('prefix', 'VARCHAR(64)', required=False),
            Field('next_hop', 'VARCHAR(64)', required=False, source_field='next-hop'),
            Field('as_path', 'VARCHAR(2048)', required=False, source_field='as-path'),
            Field('communities', 'VARCHAR(2048)', required=False, join_as_str=True),
        ]
        super().__init__(table_name, fields)

    def serialize_item(self, item: object) -> str:
        serialized = ''

        for field in self.fields:
            required = not hasattr(field, 'required') or getattr(field, 'required')
            join_as_str = hasattr(field, 'join_as_str') and getattr(field, 'join_as_str')

            if required:
                field_value = getattr(item, field.source_field)
            else:
                field_value = item._maybe_field(field.source_field)

            if join_as_str and field_value:
                field_value =  ', '.join(field_value)        

            if type(field_value) == str:
                field_value = "'" + field_value + "'"

            if not field_value:
                field_value = 'NULL'
            
            serialized += f'{field_value},'
        
        serialized = f'({serialized[:-1]})'
        
        return serialized


