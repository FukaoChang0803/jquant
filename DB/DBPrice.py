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
        if ( conn.is_connected() ):
            cursor.close()
            conn.close()
        # print("MySQL connection is closed")

