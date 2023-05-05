import requests
import json
import sqlite3
from configparser import ConfigParser


def create_db_table(conn):
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS finance_data
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 company_name TEXT NOT NULL,
                 symbol TEXT NOT NULL,
                 date TEXT NOT NULL,
                 open FLOAT,
                 high FLOAT,
                 low FLOAT,
                 close FLOAT,
                 volume INTEGER)''')


def download_finance_data(company_name, symbol):
   
    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError(f"Failed to download finance data for {company_name} ({symbol})")
    return json.loads(response.content)


def insert_or_update_data(conn, company_name, symbol, data):
   
    c = conn.cursor()
    for item in data['historical']:
        date = item['date']
        open_price = item['open']
        high_price = item['high']
        low_price = item['low']
        close_price = item['close']
        volume = item['volume']
        c.execute("SELECT * FROM finance_data WHERE company_name = ? AND symbol = ? AND date = ?", (company_name, symbol, date))
        row = c.fetchone()
        if row:
            c.execute("UPDATE finance_data SET open = ?, high = ?, low = ?, close = ?, volume = ? WHERE id = ?", (open_price, high_price, low_price, close_price, volume, row[0]))
        else:
            c.execute("INSERT INTO finance_data (company_name, symbol, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (company_name, symbol, date, open_price, high_price, low_price, close_price, volume))
    conn.commit()


if _name_ == '_main_':
    # Read configuration file
    config = ConfigParser()
    config.read('companies.cfg')

    # Connect to database
    conn = sqlite3.connect('finance.db')
    create_db_table(conn)

    # Download finance data for each company and insert into database
    for company_name, symbol in config.items('companies'):
        finance_data = download_finance_data(company_name, symbol)
        insert_or_update_data(conn, company_name, symbol, finance_data)

    conn.close()