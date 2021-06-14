from config import *
import sqlite3

connection = sqlite3.connect(DB_FILE)

cursor = connection.cursor()

cursor.execute("""
    DROP TABLE stock
""")

cursor.execute("""
    DROP TABLE stock_price
""")

connection.commit()
