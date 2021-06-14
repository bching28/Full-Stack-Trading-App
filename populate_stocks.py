from config import *
import sqlite3
import alpaca_trade_api as tradeapi

connection = sqlite3.connect(DB_FILE)

connection.row_factory = sqlite3.Row

cursor = connection.cursor()

cursor.execute("""
    SELECT symbol, name FROM stock
""")

rows = cursor.fetchall()
symbols = [row['symbol'] for row in rows]

api = tradeapi.REST(API_KEY, SECRET_KEY, base_url=BASE_URL)
assets = api.list_assets()

for asset in assets:
    try:
        if asset.symbol not in symbols and asset.status == 'active' and asset.tradable:
            print(f"Added a new stock {asset.symbol} {asset.name}")
            cursor.execute("INSERT INTO stock (symbol, name, exchange) VALUES (?, ?, ?)",
                           (asset.symbol, asset.name, asset.exchange))
    except Exception as e:
        print(asset.symbol)
        print(e)

connection.commit()