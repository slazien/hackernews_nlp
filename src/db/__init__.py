from src.db.create_db import DBCreate
from src.db.create_tables import CreateTableItems
from src.db.constants import DB_PASSWORD
from src.db.connection import DBConnection
from src.db.constants import DB_NAME_INITIAL, DB_NAME_HACKERNEWS

# Create DB if not exists
conn_initial = DBConnection(user="postgres", password=DB_PASSWORD, db_name=DB_NAME_INITIAL)
dbcreate = DBCreate(conn=conn_initial, db_name=DB_NAME_HACKERNEWS)
dbcreate.create_db()

# Create tables if not exist
conn_hackernews = DBConnection(user="postgres", password=DB_PASSWORD, db_name=DB_NAME_HACKERNEWS)
table_items = CreateTableItems(conn=conn_hackernews, db_name=DB_NAME_HACKERNEWS)
table_items.create_table()
