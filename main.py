import os
# --------------------------------
import pandas as pd
# ---------------------------------
from Util import FileUtil, SystemEnv
from Instrument import TimeSeriesTicker
from DB import DBPrice
from MarketData import yahoo_fin_Market_Data


def read_price_file():
    # SystemEnv.read_config('config.ini')
    # print(SystemEnv.g_price_file)

    for key, value in SystemEnv.g_price_file.items():
        print('{}={}'.format(key, value))

    price_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], "Yahoo_TSLA.csv")

    if not FileUtil.file_exist(price_file):
        print("Price File {} does not exist.".format(price_file))
        return
    df_price = pd.read_csv(price_file)

    return df_price
"""
Scraping Financials
"""


def get_historical_price(tickers, db_upd=True, output_file=False):
    """
    :return: DataFrame df_price
    """
    # tickers = (SystemEnv.g_tick_list[SystemEnv.ConfigSection.E_TICKER.value]).split(',')

    df_price = yahoo_fin_Market_Data.get_data(tickers, index_as_date=False)

    # price_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], "historical_price.csv")
    #
    # df_price.to_csv(price_file)

    """
            data type datetime64[ns] -> convert dt.date ---> object
            DataFrame- the data type of date column is datetime64[ns] 
            In order to match the data type of DATE, it needs to do the data type conversion
            string from timestamp : row.date.strftime('%Y-%m-%d'),
        """
    df_price['date'] = df_price['date'].dt.date
    ticker_group = df_price.groupby('ticker')
    # print( type(ticker_group)) #<class 'pandas.core.groupby.generic.DataFrameGroupBy'>

    for name, grp in ticker_group:
        grp = TimeSeriesTicker.calc_atr(grp)
        if output_file :
            price_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], name+"_historical_price.csv")
            grp.to_csv(price_file)
        if db_upd :
            DBPrice.update_price(grp)

    # return df_price


def get_balance_sheet(tickers, yearly=True,  db_upd=True, output_file=False):

    dict_balance_sheet = yahoo_fin_Market_Data.get_balance_sheet(tickers,yearly)

    if db_upd :
        DBPrice.update_balance_sheet(dict_balance_sheet)

    if output_file:
        for ticker, df_sheet in dict_balance_sheet.items():
            out_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], ticker + "_balance_sheet.csv")
            df_sheet.to_csv(out_file)


def get_income_statement(tickers, yearly=True,  db_upd=True, output_file=False):

    dict_income_statement = yahoo_fin_Market_Data.get_income_statement(tickers,yearly)

    print(dict_income_statement)

    if db_upd :
        DBPrice.update_income_statement(dict_income_statement)

    if output_file:
        for ticker, df_sheet in dict_income_statement.items():
            out_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], ticker + "_income_statement.csv")
            df_sheet.to_csv(out_file)


def get_cash_flow(tickers, yearly=True,  db_upd=True, output_file=False):

    dict_cash_flow = yahoo_fin_Market_Data.get_cash_flow(tickers,yearly)

    if db_upd :
        DBPrice.update_cash_flow(dict_cash_flow)

    if output_file:
        for ticker, df_sheet in dict_cash_flow.items():
            out_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], ticker + "_cash_flow.csv")
            df_sheet.to_csv(out_file)




def main():

    SystemEnv.read_config('c:\Temp\jquant\config.ini')
    tickers = (SystemEnv.g_tick_list[SystemEnv.ConfigSection.E_TICKER.value])
    yearly=False
    db_upd=True
    output_file=False
    # get_historical_price(tickers, db_upd=False, output_file=True)
    # get_balance_sheet(tickers, yearly, db_upd, output_file)
    # get_income_statement(tickers, yearly, db_upd,output_file)
    get_cash_flow(tickers, yearly, db_upd,output_file)
    print("Done.................")


if __name__ == '__main__':
    main()

#
# def test():
#
#     for key, value in SystemEnv.g_mysql_connection.items():
#         print('{}={}'.format(key, value))
#
#     try:
#         connection = mysql.connector.connect(host='localhost',
#                                                  database='quant',
#                                                  user='jchang',
#                                                  password='shulin0803')
#
#         cursor = connection.cursor()
#         arg = ('TSLA', '4/6/2016', 49.824, 50.424, 48.728, 49.397, 67376500, 1.695,
#                     1.882, 1.695, 2.056)
#         result_args = cursor.callproc('usp_Price_IU', arg)
#         connection.commit()
#         # arg = ('TSLA',)
#         # result_args = cursor.callproc('usp_Price_Sel', arg)
#         # print results
#         print("Printing usp_Price_IU details", result_args)
#         print("======================================")
#         for result in cursor.stored_results():
#             print(result.fetchall())
#
#     except mysql.connector.Error as error:
#             print("Failed to execute stored procedure: {}".format(error))
#     finally:
#         if (connection.is_connected()):
#             cursor.close()
#         connection.close()
#         print("MySQL connection is closed")