import psycopg as pg
import os

db = pg.connect(os.environ.get("DATABASE_URL"))
cur = db.cursor()
