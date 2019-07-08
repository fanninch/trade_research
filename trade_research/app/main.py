import pandas as pd
import mysql.connector
from mysql.connector import Error
import numpy as np
import os
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.stats import norm, skew, kurtosis

PATH = os.path.dirname(os.path.realpath(__file__))


def main():
    print ('this is main')
    try:
        # connection = mysql.connector.connect(host='localhost',
        #                                      database='testdb',
        #                                      user='root', password='tigers1')
        # if connection.is_connected():
        #     db_info = connection.get_server_info()
        #     print("Connected to mysql database", db_info)
        #     cursor = connection.cursor()
        #     cursor.execute("select * from pet;")
        #     record = cursor.fetchone()
        #     print("You're connected to - ", record)
        connection = mysql.connector.connect(host='localhost',
                                             database='testdb',
                                             user='root', password='tigers1')
        df = pd.read_sql('SELECT * FROM pet;', con=connection)
        print(df)

    except Error as e:
        print('Error while connecting to mysql', e)

    finally:
        if(connection.is_connected()):
            connection.close()

def load_data(ticker, period):
    df = pd.read_csv("/media/chuck/DATA/Dropbox/chuck/market_data/{}/{}.Last.txt".format(period, ticker.upper()), delimiter=';',
                     names=['dt', 'open', 'high','low', 'close', 'volume'],
                     dtype={'dt':np.str, 'open':np.float64, 'high':np.float64,
                            'low':np.float64,'close':np.float64, 'volume':np.int})
    # df.columns=['dt', 'open', 'high','low', 'close', 'volume']


    connection = mysql.connector.connect(host='localhost',
                                         database='marketdata',
                                         user='root', password='tigers1')

    DB_NAME = 'marketdata'

    TABLES = {}

    # TABLES['cvx_min'] = (
    #     "CREATE TABLE cvx_min ("
    #     " dt date NOT NULL,"
    #     " 'open' double NOT NULL,"
    #     " 'high' double NOT NULL,"
    #     " 'low' double NOT NULL,"
    #     " 'close' double NOT NULL,"
    #     " PRIMARY KEY ('dt')"
    #     ") ENGINE=InnoDB"
    # )
    TABLES['{}_{}'.format(ticker, period)] = ("DROP TABLE IF EXISTS {}_daily; "
                         "CREATE TABLE {}_daily (dt date NOT NULL, open double, high double, "
                         "low double, close double, volume bigint unsigned, PRIMARY KEY(dt)) ENGINE=InnoDB".format(ticker, ticker))

    cursor = connection.cursor()
    try:
        cursor.execute(TABLES["{}_{}".format(ticker, period)])
    except mysql.connector.Error as err:
        print("failed creating db: {}".format(err) )

    cursor.close()
    connection.close()

    connection = mysql.connector.connect(host='localhost',
                                         database='marketdata',
                                         user='root', password='tigers1')
    # df.to_sql(con=connection, name='cvx_min', if_exists='append', schema='mysql', index=False )
    add_data = ("INSERT INTO {}_{} "
                "(dt, open, high, low, close, volume) "
                "VALUES (%s, %s, %s, %s, %s, %s)".format(ticker, period))
    # data = (str(df.loc[0]['dt']), str(df.loc[0]['open']),
    #         str(df.loc[0]['high']), str(df.loc[0]['low']), str(df.loc[0]['close']),
    #         str(df.loc[0]['volume']))
    # print(df.loc[0]['dt'])
    # print(data)
    # cursor = connection.cursor()
    # cursor.execute(add_data, data)
    # connection.commit()
    # cursor.close()
    # connection.close()
    # print(type(str(df.loc[0]['open'])))

    cursor = connection.cursor()
    for i in range (df.shape[0]):
        data = (str(df.loc[i]['dt']), str(df.loc[i]['open']),
                str(df.loc[i]['high']), str(df.loc[i]['low']), str(df.loc[i]['close']),
                str(df.loc[i]['volume']))
        cursor.execute(add_data, data)

    connection.commit()

def get_pair_returns(sec1, sec2, period='daily'):
    print("")
    with open(PATH + "/../sql/get_pair_returns.sql", 'r') as f:
        print(PATH + "/../sql/get_pair_returns.sql")
        sql_stmt = f.read()

    sql_stmt = sql_stmt.replace('sec1', sec1).replace('sec2', sec2).replace('period', period)
    con = get_connection()
    df = pd.read_sql(sql_stmt, con=con)
    df['ret_spread'] = df['{}_return'.format(sec1)] - df['{}_return'.format(sec2)]
    f, ax = plt.subplots(figsize=(16, 14))
    sns.distplot(df['ret_spread'], fit=norm, color='b')
    ax.set(ylabel='Frequency')
    ax.set(xlabel='return_spread')
    plt.show()
    print(skew(df['ret_spread']))
    print(kurtosis(df['ret_spread']))
    print(df['ret_spread'].mean())
    print(df)



def get_connection():
    return mysql.connector.connect(host='localhost', database='marketdata',
                                             user='root', password='tigers1')

if __name__ == '__main__':
    # tickers = ['cvx', 'xom']
    # period = 'daily'
    # for ticker in tickers:
    #     load_data(ticker, period)

    get_pair_returns("aapl", "xom")
    # load_data('aal', 'daily')