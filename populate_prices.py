import alpaca_trade_api as tradeapi
import sqlite3
import datetime
from pytz import timezone
from config import *

def convert_date_to_iso(month, day, year, hour=0, min=0, second=0):
    date_time_iso = datetime.datetime(year, month, day, hour, min, second, tzinfo=timezone('US/Pacific')).isoformat()
    return date_time_iso

def get_current_time_iso():
    return datetime.datetime.now(tz=timezone('US/Pacific')).isoformat()

connection = sqlite3.connect(DB_FILE)

connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    SELECT id, symbol, name FROM stock
""")

rows = cursor.fetchall()

symbols = []
stock_dict = {}

for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']

api = tradeapi.REST(API_KEY, SECRET_KEY, base_url=BASE_URL)

chunk_size = 200
start_date = convert_date_to_iso(5, 1, 2021)
end_date = get_current_time_iso()

for i in range(0, len(symbols), chunk_size):
    symbol_chunk = symbols[i:i+chunk_size]

    barsets = api.get_barset(symbols=symbol_chunk, timeframe='1D')

    for symbol in barsets:
        print(f'processing data for symbol: {symbol}')

        for bar in barsets[symbol]:
            stock_id = stock_dict[symbol]
            cursor.execute("""
                INSERT INTO stock_price (stock_id, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (stock_id, bar.t.date(), bar.o, bar.h, bar.l, bar.c, bar.v))

connection.commit()
