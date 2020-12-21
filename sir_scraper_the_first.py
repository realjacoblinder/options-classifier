from yahoo_fin.options import get_options_chain
from yahoo_fin.stock_info import get_data as get_stock_data
from datetime import datetime
from datetime import date
import os
import time
import pandas as pd
import dateutil.parser as dp
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Text, String, DateTime, Float

def get_expiry_from_name(c_name, ticker):
    offset = len(ticker)
    expiry = c_name[offset:offset+3] + c_name[offset+3:offset+5] + c_name[offset+5:offset+6]
    expiry = expiry[2:] + expiry[0:2]
    return dp.parse(expiry)

load_dotenv()

DB_PASS = os.getenv("DB_PASS")
DB_USER = os.getenv("DB_USER")
DB_HOST = os.getenv("DB_HOST")

engine = create_engine(f"mysql://{DB_USER}:{DB_PASS}@{DB_HOST}/options")
conn = engine.connect()

table_name = 'options_data'

hour = str(datetime.now().time().hour)
base_dir = '/home/pi/options/' # change as needed
tickers = pd.read_csv(base_dir + 'tickers_csv.csv')['TICKER']
#tickers=['AAL']
for ticker in tickers:
    try:
        chain = get_options_chain(ticker)
        puts = chain['puts']
        calls = chain['calls']
    except ValueError:
        file = open(base_dir + 'errors/error ' + str(date.today()) + '.txt', 'a')
        file.write(ticker + ' ' + hour + '\n')
        file.close()

    
    puts['contract'] = 'P'
    calls['contract'] = 'C'
    
    data = pd.concat([puts,calls])
    data.reset_index(drop=True,inplace=True)
    data['pull_timestamp'] = str(datetime.now())
    
    expiry = get_expiry_from_name(data['Contract Name'][0], ticker)
        
    data['expiry'] = expiry

    data['days_to_expiry'] = (data['expiry'] - datetime.today()).apply(lambda x: x.days + 1)


    data['Volume'] = data['Volume'].apply(lambda x: '0' if x == '-' else x)
    data['Open Interest'] = data['Open Interest'].apply(lambda x: '0' if x == '-' else x)
    data = data[data['Implied Volatility'] != '-']
    data['% Change'] = data['% Change'].apply(lambda x: '0' if x == '-' else x)
    data = data[data['Last Trade Date'] != '-']

    try:
        close = get_stock_data(ticker, date.today(), index_as_date=False)['adjclose'][0]
    except:
        close = -1
    
    data['close_on_pull'] = close

    data['Implied Volatility'] = data['Implied Volatility'].apply(lambda x: float(x.replace('%','').replace(',',''))/100)
    data['% Change'] = data['% Change'].apply(lambda x: float(x.replace('%','').replace(',',''))/100)
    data['Last Trade Date'] = data['Last Trade Date'].apply(dp.parse)
    data['ticka'] = ticker

    data.rename(columns = {'% Change': 'pct_change', 'Contract Name':'contract_name', 'Last Trade Date':'last_trade_date', 'Strike':'strike',
    'Last Price':'last_price','Bid':'bid','Ask':'ask','Change':'change','Volume':'volume','Open Interest':'open_interest',
    'Implied Volatility':'implied_volatility'}, inplace=True)

    data.to_sql(
        table_name,
        conn,
        if_exists='append',
        index=False,
        dtype={
            'ticka':String(10),
            'contract_name':String(50),
            'last_trade_date':DateTime,
            'strike':Float,
            'last_price':Float,
            'did':Float,
            'ask':Float,
            'change':Float,
            'pct_change':Float,
            'volume':Integer,
            'open_interest':Integer,
            'implied_volatility':Float,
            'pull_timestamp':DateTime,
            'close_on_pull':Float,
            'contract':Text,
            'expiry':DateTime, 
            'days_to_expiry':Integer
        }
    )