###https://github.com/sooshie/Security-Data-Analysis/blob/master/Lab_1/Lab_1-Solutions.ipynb


import pandas as pd
import sklearn

from sklearn.model_selection import train_test_split

##test_set= train_test_split(data, test_size=0.2, random_state=42)

if __name__ == '__main__':
    print("TAREA")
    logfile_header = 'resources/conn_sample_header.log'
    # read the log file with header
    conn_df_header = pd.read_csv(logfile_header,
                                 sep=" ")
    logfile_no_header = 'conn.log'
    conn_df_no_header = pd.read_csv(logfile_no_header,
                                    sep=",", header=None,
                                    names=['ts', 'uid', 'id_orig_h', 'id_orig_p',
                                           'id_resp_h', 'id.resp_p',
                                           'proto', 'service', 'duration', 'orig_bytes', 'resp_bytes',
                                           'conn_state', 'local_orig', 'missed_bytes',
                                           'history', 'orig_pkts', 'orig_ip_bytes', 'resp_pkts', 'resp_ip_bytes',
                                           'tunnel_parents', 'threat', 'sample'])

    print(conn_df_no_header.size)

    test_set = train_test_split(conn_df_no_header, test_size=0.2, random_state=42)
    #print(test_set)
    #print(test_set[0])
    #print(test_set)

    ######## ANALYSIZING DATASET #########################

    # basic check of input,(values = head or  tail)


    '''
    print(test_set.head())

    # shape of file
    print(test_set.shape)

    # basic info of structure of dataset
    print(test_set.info())

    # data sumarization
    print(test_set.describe())

    # data types
    print(test_set.dtypes)

    # Return with the unique values of ‘id_orig_h' attribute.
    print(test_set.id_orig_h.unique())
    # or using this sintaxe -> print(conn_df_header['id_orig_h'].unique())

    # count the instances
    print(test_set.id_orig_h.value_counts())
    '''