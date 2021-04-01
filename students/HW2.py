import pandas as pd
import pyflux as pf
import matplotlib.pyplot as plt
from datetime import datetime
import arima_utils
from scipy import stats
import numpy as np


def len2(val):
    return len(val)

def malware_resp_mime_types(value):
    executable_types = set(['application/x-dosexec', 'application/octet-stream', 'binary', 'application/vnd.ms-cab-compressed'])
    if value in executable_types:
        return 1
    else:
        return 0
def malware_user_agent(value):
    common_exploit_types = set(['application/x-java-applet', 'application/pdf', 'application/zip', 'application/jar',
                                'application/x-shockwave-flash'])
    if value in common_exploit_types:
        return 1
    else:
        return 0

def malware_resp(conn):
    conn['check_respmimetypes']=conn['resp_mime_types'].apply(malware_resp_mime_types)
    arima_utils.adfuller_test(conn['check_respmimetypes'])
    arima_utils.plot_series(conn['check_respmimetypes'],"check respmime")
    arima_utils.plot_pacf(conn['check_respmimetypes'])
    arima_utils.plot_acf(conn['check_respmimetypes'])
    #p  6.48504532572564e-24
    #pacf x=90 y=-.03685
filename='resources/http-1.log'
conn = pd.read_csv(filename, sep="\t", header=None,
                       names=[ 'ts', 'uid', 'id_orig_h', 'id_orig_p', 'id_resp_h', 'id_resp_p',
                             'trans_depth', 'method', 'host', 'uri', 'referrer', 'user_agent',
                             'request_body_len', 'response_body_len', 'status_code', 'status_msg',
                             'info_code', 'info_msg', 'filename', 'tags', 'username',
                             'password', 'proxied', 'orig_fuids', 'orig_mime_types', 'resp_fuids',
                             'resp_mime_types', 'sample'])
conn['ts'] = [
        datetime.fromtimestamp(float(date))
        for date in conn['ts'].values
    ]




#conn['status_code']=pd.to_numeric(conn.status_code.astype(str).str.replace(',',''), errors='coerce').fillna(0).astype(int)
#conn['method']=pd.to_numeric(conn.method.astype(str).str.replace(',',''), errors='coerce').fillna(0).astype(int)

#TASK 1
#conn20=conn.head( int(4023*.20))
conn80=conn.head( int(4023*.80) )





#TASK 2
#Because the data is was captured through a period of time. if we were to randomize it, we would lose the patterns shown during the period of time the data was recorded.



def ANY_size(conn80,ANY,shiftvalue):
    new_size=ANY+"_size"
    new_shift=ANY+"_shift_"+str(shiftvalue)
    conn80[new_size]=conn80[ANY].apply(len)
    print(conn80[new_size])
    if shiftvalue>0:
        conn80[new_shift]=conn80[new_size]-conn80[new_size].shift(shiftvalue)
        arima_utils.adfuller_test(conn80[new_shift].dropna())
        arima_utils.plot_series(conn80[new_shift], "uri_size shift=" + str(shiftvalue))
    else:
        fix=conn80[np.abs(conn80[new_size] - conn80[new_size].mean()) <= (1 * conn80[new_size].std())]

        #arima_utils.adfuller_test(fix[new_size])
        #arima_utils.plot_series(fix[new_size],new_size)
        #arima_utils.plot_pacf(fix[new_size])
        #arima_utils.plot_acf(fix[new_size])
    return fix

conn2=ANY_size(conn80,'uri',0)
#pacf x=49 y=-.0485
#acf x=69 y=-.003

p=49
q=69







# GET DATA
# URI SIZE
# REMOVE EVERYTHING ABOVE/BELOW 3 STANDARD DEVIATIONS
# P<=.05
# PLOT TO GET AR MA
# MODEL








print(conn2['uri_size'])
model = pf.ARIMA(data=conn2,
                   ar=p, ma=q, integ=0, target='uri_size')
x = model.fit("PML")
print("model")
print(x.summary())

print(x.scores)


model.plot_fit()
plt.show()













#TASK 3
def statuscode():
    test1='status_code'
    arima_utils.adfuller_test(conn80[test1])
    arima_utils.plot_series(conn80[test1],test1)

#Data captured with Zeek

def tes2(conn):
    final2 = conn.loc[(conn['id_orig_p'] != 80) & (conn['id_resp_p'] != 80) & (conn['id_orig_p'] != 8080) & (
    conn['id_resp_p'] != 8080)]
    final3 = final2.groupby([final2['ts'].dt.to_period('M')])

    print(conn.assign(to_sum=conn.a.gt(0).astype(int)).groupby('date').to_sum.sum())



