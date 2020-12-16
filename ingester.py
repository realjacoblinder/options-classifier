import pandas as pd
import yahoo_fin.stock_info as si
import dateutil.parser as dp
import os
import yahoo as lib
from tqdm import tqdm

pd.set_option('mode.chained_assignment', None)

def get_expiry_from_name(c_name, ticker):
    offset = len(ticker)
    expiry = c_name[offset:offset+3] + c_name[offset+3:offset+5] + c_name[offset+5:offset+6]
    expiry = expiry[2:-1] + expiry[0:2]
    return expiry

def is_assigned(row):
    close_day = si.get_data(row['Ticker'], start_date=row['Expiry'], end_date=row['Expiry'], index_as_date=False)
    close = close_day.loc[0,'adjclose']
    if row['Contract'] == 'P':
        if close < row['Strike']:
            return 1
    else:
        if close > row['Strike']:
            return 0

def active_contracts(row):
    last = row['Last Trade Date']
    pull = row['Pull Timestamp']
    if last.day == pull.day & last.month == pull.month & last.year == pull.year:
        return 1
    else:
        return 0


def do_the_loop(chain):
    chain = chain[['Contract Name','Strike', 'Last Trade Date', 'Pull Timestamp', 'Implied Volatility']]
    chain['Pull Timestamp'] = chain['Pull Timestamp'].apply(dp.parse)
    chain['Last Trade Date'] = chain['Last Trade Date'].apply(dp.parse)
    chain['Active Contracts'] = chain.apply(active_contracts, axis = 1)
    expiry = get_expiry_from_name(chain['Contract Name'][0], ticker)
    pull_date = chain['Pull Timestamp'][0]
    chain = chain[chain['Active Contracts'] == 1]
    chain['Expiry'] = expiry
    chain['Ticker'] = ticker
    band_data = lib.get_band(ticker, start_date=pull_date, band_age=20)
    chain['MA'] = band_data['MA'][0]
    chain['STD'] = band_data['STD'][0]
    return chain, band_data['Upper'][0], band_data['Lower'][0]

master_data = pd.DataFrame()

ticker_list = os.listdir('im_vol')
for ticker in tqdm(ticker_list):
    path = 'im_vol/'+ticker
    for file in os.listdir(path+'/calls'):        
        chain = pd.read_csv(path+'/calls/'+file)
        chain, upper, lower = do_the_loop(chain)
        chain['Contract'] = "C"
        chain['Safe'] = chain['Strike'] > upper
    master_data = pd.concat([master_data, chain])
    for file in os.listdir(path+'/puts'):
        chain = pd.read_csv(path+'/puts/'+file)
        chain, upper, lower = do_the_loop(chain)
        chain['Contract'] = "P"
        chain['Safe'] = chain['Strike'] < lower
    master_data = pd.concat([master_data, chain])

master_data.to_csv('master_data.csv')
master_data.to_json('master_data.json')
