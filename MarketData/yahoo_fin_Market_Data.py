# all methods can be imported at once
# from yahoo_fin.stock_info import *
# or
import pandas as pd
import yahoo_fin.stock_info as si
from functools import reduce


def get_data(tickers, start_date=None, end_date=None, index_as_date=True, interval='1d'):
    """
    :param start_date:
    :param end_date:
    :param index_as_date:
    :param interval:
    :param tickers is a list of Equity tickers:
    :return :  
    """
    price_data = {ticker: si.get_data(ticker.strip(), start_date, end_date, index_as_date, interval)
                  for ticker in tickers}
    combined = reduce(lambda x, y: x.append(y), price_data.values())
    return combined

# def get_balance_sheet(ticker, yearly = True):

# ticker='AAPL'
# ticker_file = "c:/temp/yahoo_fin_{}.csv".format(ticker)
# data = si.get_data(ticker,start_date = '01/01/2000' )
# data.to_csv(ticker_file)
# print(data.tail())

# Analysts = si.get_analysts_info(ticker)
# print (Analysts)

# balance_sheet = si.get_balance_sheet(ticker,yearly = False)
# print (balance_sheet)
