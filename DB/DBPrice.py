from datetime import datetime
import pandas as pd
import mysql.connector
from mysql.connector import Error

from Instrument import TimeSeriesTicker
from DB import dbconnect


def update_price(df_price):
    try:
        conn = dbconnect.connect()
        cursor = conn.cursor()
        for index, row in df_price.iterrows():
            parameters = [row.ticker, row.date, row.open, row.high, row.low, row.close, row.adjclose,
                          row.returns, row.volume, row.atr21_ewm, row.atr21_ma, row.atr14_ewm, row.atr14_ma]
            cursor.callproc('usp_Price_IU', parameters)
        conn.commit()
    except Error as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
        # print("MySQL connection is closed")


def update_balance_sheet(dict_balance_sheet):

    try:
        conn = dbconnect.connect()
        cursor = conn.cursor()

        for ticker, df_sheet in dict_balance_sheet.items():
            df_sheet.fillna(0, inplace=True)
            dict_sheet = df_sheet.to_dict()
            for endDate, row in dict_sheet.items():
                # print( type(endDate)) # <class 'pandas._libs.tslibs.timestamps.Timestamp'>
                dt_endDate =  endDate.to_pydatetime()
                # print( type(dt_endDate))

                for item, vlue in row.items():
                    parameters = [ticker, dt_endDate, item, vlue]
                    cursor.callproc('usp_BalanceSheet_IU', parameters)

        conn.commit()
    except Error as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
        # print("MySQL connection is closed")
