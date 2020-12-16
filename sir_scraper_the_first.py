from yahoo_fin.options import get_options_chain
from datetime import date
from datetime import datetime
import os
import time
import pandas as pd

hour = str(datetime.now().time().hour)
base_dir = str(os.path.expanduser("~")).replace('\\', '/') + '/options/'
stonks_dir = base_dir + 'im_vol/'
file_str = str(date.today()) + '.' + hour + '.csv'

if not os.path.exists(stonks_dir):
    os.makedirs(stonks_dir)

tickers = pd.read_csv(base_dir + 'tickers_csv.csv')['TICKER']
for ticker in tickers:
    try:
        chain = get_options_chain(ticker)
        puts = chain['puts']
        calls = chain['calls']

        puts['Pull Timestamp'] = str(datetime.now())
        calls['Pull Timestamp'] = str(datetime.now())

        if os.path.exists(stonks_dir + ticker):
            puts.to_csv(stonks_dir + ticker + '/puts/' + file_str)
            calls.to_csv(stonks_dir + ticker + '/calls/' + file_str)
        else:
            os.mkdir(stonks_dir + ticker)
            os.mkdir(stonks_dir + ticker + '/puts')
            os.mkdir(stonks_dir + ticker + '/calls')

            puts.to_csv(stonks_dir + ticker + '/puts/' + file_str)
            calls.to_csv(stonks_dir + ticker + '/calls/' + file_str)
    except ValueError:
        file = open(base_dir + 'error ' + str(date.today()) + '.txt', 'a')
        file.write(ticker + ' ' + hour + '\n')
        file.close()
    time.sleep(5)
