import os

DB_PROTOCOL = os.environ.get('DB_PROTOCOL', 'postgresql+psycopg2')
DB_USER = os.environ.get('DB_USER', 'user')
DB_PWD = os.environ.get('DB_PWD', 'password')
DB_HOST = os.environ.get('DB_HOST', 'postgres')
DB_PORT = os.environ.get('DB_PORT', 5432)
DB_NAME = os.environ.get('DB_NAME', 'postgres')
DB_URL = f'{DB_PROTOCOL}://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'



