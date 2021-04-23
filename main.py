import os
# --------------------------------
import pandas as pd
import mysql.connector
from mysql.connector import Error
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


def get_historical_price():
    """
    :return: DataFrame df_price
    """
    tickers = (SystemEnv.g_tick_list[SystemEnv.ConfigSection.E_TICKER.value]).split(',')
    df_price = yahoo_fin_Market_Data.get_data(tickers, index_as_date=False)

    # price_file = os.path.join(SystemEnv.g_price_file['sourcefolder'], "historical_price.csv")
    #
    # df_price.to_csv(price_file)

    return df_price

def calc_atr():
        pass


def db_update_price():
        pass

def test():

    for key, value in SystemEnv.g_mysql_connection.items():
        print('{}={}'.format(key, value))


    try:
        connection = mysql.connector.connect(host='localhost',
                                                 database='quant',
                                                 user='jchang',
                                                 password='shulin0803')

        cursor = connection.cursor()
        arg = ('TSLA', '4/6/2016', 49.824, 50.424, 48.728, 49.397, 67376500, 1.695,
                    1.882, 1.695, 2.056)
        result_args = cursor.callproc('usp_Price_IU', arg)
        connection.commit()
        # arg = ('TSLA',)
        # result_args = cursor.callproc('usp_Price_Sel', arg)
        # print results
        print("Printing usp_Price_IU details", result_args)
        print("======================================")
        for result in cursor.stored_results():
            print(result.fetchall())

    except mysql.connector.Error as error:
            print("Failed to execute stored procedure: {}".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
        connection.close()
        print("MySQL connection is closed")


def main():

    SystemEnv.read_config('./config.ini')
    df_price = get_historical_price()

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
        DBPrice.update_price(grp)


if __name__ == '__main__':
    main()


