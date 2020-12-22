from tqdm import tqdm
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

#import sys

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

table_name = 'ingester_test'

ticker_list = os.listdir('im_vol')
for ticker in tqdm(ticker_list):
    path = 'im_vol/'+ticker
    for file in os.listdir(path+'/calls'):
        if int(file.split('.')[-2]) < 16:
            continue # only take after market pulls   
        f_path = path+'/calls/'+file
        data = pd.read_csv(f_path)
        data = data.drop(columns='Unnamed: 0')
        data['contract'] = 'C'
        #same from here
        data['Pull Timestamp'] = data['Pull Timestamp'].apply(dp.parse)
        data = data[data['Last Trade Date'] != '-']
        data['Last Trade Date'] = data['Last Trade Date'].apply(dp.parse)
        
        expiry = get_expiry_from_name(data['Contract Name'][0], ticker)
        data['expiry'] = expiry
        data['days_to_expiry'] = data['expiry'] - data['Pull Timestamp']
        data['days_to_expiry'] = data['days_to_expiry'].apply(lambda x: x.days)

        data['Volume'] = data['Volume'].apply(lambda x: 0 if x == '-' else x)
        data['Open Interest'] = data['Open Interest'].apply(lambda x: '0' if x == '-' else x)
        data = data[data['Implied Volatility'] != '-']
        data['% Change'] = data['% Change'].apply(lambda x: '0' if x == '-' else x)
        
        data['Ask'] = data['Ask'].apply(lambda x: 0 if type(x) != float and type(x) != int else float(x))
        data['Bid'] = data['Bid'].apply(lambda x: 0 if type(x) != float and type(x) != int else float(x))
        data['Change'] = data['Change'].apply(lambda x: 0 if type(x) != float and type(x) != int else float(x))
        data['Last Price'] = data['Last Price'].apply(lambda x: 0 if type(x) != float and type(x) != int else float(x))

        try:
            close = get_stock_data(ticker, data['Pull Timestamp'][0], index_as_date=False)['adjclose'][0]
        except:
            close = -1
        
        data['close_on_pull'] = close
        data['Implied Volatility'] = data['Implied Volatility'].apply(lambda x: float(x.replace('%','').replace(',',''))/100)
        data['% Change'] = data['% Change'].apply(lambda x: float(x.replace('%','').replace(',',''))/100)
        data['ticka'] = ticker

        # make sure other columns are the correct format here - bid, ask, change, last price

        data.rename(columns = {'% Change': 'pct_change', 'Contract Name':'contract_name', 'Last Trade Date':'last_trade_date', 'Strike':'strike',
    'Last Price':'last_price','Bid':'bid','Ask':'ask','Change':'change','Volume':'volume','Open Interest':'open_interest',
    'Implied Volatility':'implied_volatility', 'Pull Timestamp':'pull_timestamp'}, inplace=True)

        data.to_csv('temp.csv')
        
        try:
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
                    'bid':Float,
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
        except:
            print(f"Failure to write at {f_path}")
            with open("errors.txt", 'a') as f:
                f.write(f"Failure to write at {f_path}")

    for file in os.listdir(path+'/puts'):
        
        if int(file.split('.')[-2]) < 16:
            continue # only take after market pulls 
        
        f_path = path+'/puts/'+file

        data = pd.read_csv(f_path)
        data = data.drop(columns='Unnamed: 0')
        data['contract'] = 'P'
        
        # same from here
        data['Pull Timestamp'] = data['Pull Timestamp'].apply(dp.parse)
        data = data[data['Last Trade Date'] != '-']
        data['Last Trade Date'] = data['Last Trade Date'].apply(dp.parse)
        
        expiry = get_expiry_from_name(data['Contract Name'][0], ticker)
        data['expiry'] = expiry
        data['days_to_expiry'] = data['expiry'] - data['Pull Timestamp']
        data['days_to_expiry'] = data['days_to_expiry'].apply(lambda x: x.days)

        data['Volume'] = data['Volume'].apply(lambda x: 0 if x == '-' else x)
        data['Open Interest'] = data['Open Interest'].apply(lambda x: '0' if x == '-' else x)
        data = data[data['Implied Volatility'] != '-']
        data['% Change'] = data['% Change'].apply(lambda x: '0' if x == '-' else x)
        
        data['Ask'] = data['Ask'].apply(lambda x: 0 if type(x) != float and type(x) != int else float(x))
        data['Bid'] = data['Bid'].apply(lambda x: 0 if type(x) != float and type(x) != int else float(x))
        data['Change'] = data['Change'].apply(lambda x: 0 if type(x) != float and type(x) != int else float(x))
        data['Last Price'] = data['Last Price'].apply(lambda x: 0 if type(x) != float and type(x) != int else float(x))

        try:
            close = get_stock_data(ticker, data['Pull Timestamp'][0], index_as_date=False)['adjclose'][0]
        except:
            close = -1
        
        data['close_on_pull'] = close
        data['Implied Volatility'] = data['Implied Volatility'].apply(lambda x: float(x.replace('%','').replace(',',''))/100)
        data['% Change'] = data['% Change'].apply(lambda x: float(x.replace('%','').replace(',',''))/100)
        data['ticka'] = ticker

        # make sure other columns are the correct format here - bid, ask, change, last price

        data.rename(columns = {'% Change': 'pct_change', 'Contract Name':'contract_name', 'Last Trade Date':'last_trade_date', 'Strike':'strike',
    'Last Price':'last_price','Bid':'bid','Ask':'ask','Change':'change','Volume':'volume','Open Interest':'open_interest',
    'Implied Volatility':'implied_volatility', 'Pull Timestamp':'pull_timestamp'}, inplace=True)

        data.to_csv('temp.csv')
        
        try:
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
                    'bid':Float,
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
        except:
            print(f"Failure to write at {f_path}")
            with open("errors.txt", 'a') as f:
                f.write(f"Failure to write at {f_path}")