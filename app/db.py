from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from . import config

# Opened/closed in the FastAPI lifespan (see main.py)
pool = ConnectionPool(
    config.DATABASE_URL,
    min_size=1,
    max_size=10,
    kwargs={"row_factory": dict_row},
    open=False,
)