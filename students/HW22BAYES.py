import pandas as pd
import pyflux as pf
import matplotlib.pyplot as plt
from datetime import datetime
import arima_utils
from scipy import stats
import numpy as np
from sklearn.model_selection import train_test_split

import time

#WORK
#method = 'PML'
#method = 'Laplace'
#method = 'BBVI'
#method = "MLE"

#NOT WORKING
#method = 'OLS'

def read_file():
    print("HW2")
    filename = 'resources/http-1.log'
    conn = pd.read_csv(filename, sep="\t", header=None,
                       names=['ts', 'uid', 'id_orig_h', 'id_orig_p', 'id_resp_h', 'id_resp_p',
                              'trans_depth', 'method', 'host', 'uri', 'referrer', 'user_agent',
                              'request_body_len', 'response_body_len', 'status_code', 'status_msg',
                              'info_code', 'info_msg', 'filename', 'tags', 'username',
                              'password', 'proxied', 'orig_fuids', 'orig_mime_types', 'resp_fuids',
                              'resp_mime_types', 'sample'])
    conn['ts'] = [
        datetime.fromtimestamp(float(date))
        for date in conn['ts'].values
    ]

    # train_set, test_set = train_test_split(conn, test_size=0.8)
    # print(train_set)
    # print(len(train_set))
    # 20% OF DATASET
    #train_set = conn.head(int(4023 * .20))

    train_set1 = conn[0:804]
    train_set2 = conn[804:1608]
    train_set3 = conn[1608:1708]
    train_set4 = conn[2412:3216]
    train_set5 = conn[3216:3316]


    #print(type(train_set1[['request_body_len', 'response_body_len']]))
    #print(train_set1[['request_body_len', 'response_body_len']].describe())
    #print(train_set2[['request_body_len', 'response_body_len']].describe())
    #print(train_set3[['request_body_len', 'response_body_len']].describe())
    #print(train_set4[['request_body_len', 'response_body_len']].describe())
    #print(train_set5[['request_body_len', 'response_body_len']].describe())


    return train_set2, train_set4, train_set3, train_set5


def predict_request_body_len(train_set, test_set, method):

    arima_utils.adfuller_test(train_set['request_body_len'].dropna())
    arima_utils.plot_series(train_set['request_body_len'], 'Original Series')
    '''
    train_set['Value First Difference'] = train_set['request_body_len'] - train_set['request_body_len'].shift(1)
    #dropdna, borra todos los vacios
    arima_utils.adfuller_test(train_set['Value First Difference'].dropna())
    arima_utils.plot_series(train_set['Value First Difference'], 'Value First Difference')
    '''
    arima_utils.plot_pacf(train_set['request_body_len'])
    arima_utils.plot_acf(train_set['request_body_len'])
    # use request_body_len, response_body_len
    # usar p = 12 (intento original)
    # usar q = 34 (intento original)
    p = 2
    q = 1

    start_time = time.time()
    print("STARTING TIMER REQUEST ", method)

    model = pf.ARIMA(data=train_set,
                     ar=p, ma=q, integ=0, target='request_body_len')
    x = model.fit(method=method)



    end_time = time.time()
    total_time = end_time - start_time
    print("TIME:          ", total_time)

    # PRINT DATA
    #print(x.summary())
    print(x.scores)
    model.plot_fit()
    plt.show()

    # model.plot_predict_is(h=30)
    # firstRegister = conn.head(30)
    #plt.plot(test_set['ts'], test_set['request_body_len'])
    #model.plot_predict_is(h=100, past_values=40)
    #print(model.predict(h=100))

    start_time = time.time()
    print("STARTING TIMER, PREDICT REQUEST  ", method)
    plt.plot(test_set.index, test_set['request_body_len'], label='REAL', color='pink')
    plt.plot(model.predict(h=100), label ='PREDICTION', color='cyan')
    plt.legend(['REAL','PREDICTION'])
    # model.plot_predict(h=200, past_values=40)
    # plt.plot(firstRegister['ts'], firstRegister['request_body_len'])
    end_time = time.time()
    total_time = end_time - start_time
    print("TIME:          ", total_time)
    plt.show()


def predict_response_body_len(train_set, test_set, method):
    arima_utils.adfuller_test(train_set['response_body_len'])
    arima_utils.plot_series(train_set['response_body_len'], 'Original Series')
    arima_utils.plot_pacf(train_set['response_body_len'])
    arima_utils.plot_acf(train_set['response_body_len'])
    # use request_body_len, response_body_len

    # usar p = 8 (intento original)
    # usar q = 7 (intento original)
    p = 2
    q = 8

    start_time = time.time()
    print("STARTING TIMER, RESPONSE  ", method)

    model = pf.ARIMA(data=train_set,
                     ar=p, ma=q, integ=0, target='response_body_len')
    x = model.fit(method=method)



    end_time = time.time()
    total_time = end_time - start_time
    print("TIME:          " , total_time)
    #PRINT DATA
    #print(x.summary())
    print(x.scores)
    model.plot_fit()
    plt.show()
    # model.plot_predict_is(h=30)
    # firstRegister = conn.head(30)
    start_time = time.time()
    print("STARTING TIMER PREDICT RESPONSE  ", method)

    plt.plot(test_set.index, test_set['response_body_len'], label='REAL', color='pink')
    plt.plot(model.predict(h=100), label='PREDICTION', color='cyan')
    plt.legend(['REAL', 'PREDICTION'])

    #end_time = time.time()
    #total_time = end_time - start_time
    #print("TIME: " , total_time)

    end_time = time.time()
    total_time = end_time - start_time
    print("TIME:          ", total_time)

    # model.plot_predict(h=200, past_values=40)
    # plt.plot(firstRegister['ts'], firstRegister['response_body_len'])
    plt.show()


if __name__ == '__main__':
    train_set1, train_set2, test_set3, test_set5 = read_file()
    print("PML TEST")
    predict_request_body_len(train_set1, test_set3, 'PML')
    predict_response_body_len(train_set2, test_set5, 'PML')
    print("LAPLACE TEST")
    predict_request_body_len(train_set1, test_set3, 'Laplace')
    predict_response_body_len(train_set2, test_set5, 'Laplace')
    print("BBVI TEST")
    predict_request_body_len(train_set1, test_set3, 'BBVI')
    predict_response_body_len(train_set2, test_set5, 'BBVI')
    print("MLE TEST")
    predict_request_body_len(train_set1, test_set3, 'MLE')
    predict_response_body_len(train_set2, test_set5, 'MLE')
    print("OLS TEST, this will fail")
    predict_request_body_len(train_set1, test_set3, 'OLS')
    predict_response_body_len(train_set2, test_set5, 'OLS')
