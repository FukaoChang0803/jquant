from os import path
import pandas as pd
from datetime import datetime
import dateutil
from mysql.connector import Error
from DB import dbconnect

def transactionImport( filename):
    try:
        """
        Two ways to convert string(object) to pandas datetype  datetime64 dtype 
        1. using read_sv(parse_dates=['Col-name,']
        2. using  df['DATE'] = pd.to_datetime(df['DATE']) after read csv file
        """
        # headers=['DATE','TRANSACTION ID','DESCRIPTION','QUANTITY','SYMBOL','PRICE','COMMISSION','AMOUNT','REG FEE']

        # 0.converting data type other than date by using dtype={ 'col':dtype }
        # dtypes = [datetime.datetime, str, str, float,str,float,float,float,float]
        dtypes = {'TRANSACTION ID': 'str'}

        # 1.) using   parse_dates=['DATE']
        df = pd.read_csv( filename, dtype=dtypes, parse_dates=['DATE'] )
        df['DATE'] = df['DATE'].dt.date

        # 2.)   method using pd.to_datetime(df['DATE'])
        # df = pd.read_csv( filename)
        # 2.1)  pd.to_datetime returns  a  Series of datetime64[ns]   dtype
        # df['DATE'] = pd.to_datetime(df['DATE']).dt.date

        df.fillna(0,inplace=True)

        # print(df.info())

        # 1.  df['DATE']
        # s string Object  converted to  datetime64[ns] only match MySQL Date type
        #     df['DATE']= pd.to_datetime(df['DATE'])
        # 2.  Convert  datetime64[ns] to Python DateTim - not working strptime( str, , '%y-%m-%d')
        # df['DATE']=datetime.strptime(df['DATE'], '%y-%m-%d')
        # df['DATE']= df['DATE'].apply(lambda d: datetime.strptime(d, '%m/%d/%Y'))

        # print(df['Date'].apply(lambda x: x.strftime('%Y-%m-%d')))
        # df['DATE'] = [val.to_pydatetime() for val in df['DATE']]
        # print(df.info())
        # datetimes = [val.to_pydatetime() for val in df['DATE']]
        # print(datetimes)
    except FileExistsError as e:
        print("Cannot find the file : {} {}".format(filename, e))

    _update_IU(df)

def _update_IU(df):
    try:
        conn = dbconnect.connect()
        cursor = conn.cursor()
        for index, row in df.iterrows():
            BuySell = 'Buy' if row['AMOUNT'] < 0 else 'Sell'
            parameters = [ row['DATE']  , row['TRANSACTION ID'], row['DESCRIPTION'],
                          row['SYMBOL'], BuySell, row['QUANTITY'], row['PRICE'],
                          row['COMMISSION'], row['AMOUNT'], row['REG FEE']]
            cursor.callproc('usp_TD_Transaction_IU', parameters)
        conn.commit()
    except Error as e:
        print("Failed to execute stored procedure: {}".format(e))
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
        # print("MySQL connection is closed")

if __name__ == '__main__':
    data_path = "C:/Users/Fukao Chang/OneDrive/_股票/TD Ameritrade"
    data_file="transactions_2006.csv"
    transactionImport( path.join(data_path, data_file)  )
