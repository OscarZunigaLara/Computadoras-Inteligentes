import pandas as pd
import random


from datetime import datetime



def pd_part1():
    filename = "samplesmall.log"
    conn = pd.read_csv(filename, sep="\t",header=None, names=['ts','uid','id_origin_h','id_origin_p','id_resp_h','id_resp_p','protocol','service','duration',
                                                             'og_bytes','resp_bytes','conn_state','local_orig','missed_bytes','history','og_pkts','orig_ip_bytes',
                                                             'resp_pkts','resp_ip_bytes','tunnel_parents','threat','sample'])

    conn['ts'] = [
        datetime.fromtimestamp(float(date))
        for date in conn['ts'].values
    ]
    final = conn.loc[(conn['id_origin_p'] != 80) | (conn['id_resp_p'] !=80 )]
    print(final.groupby(["id_origin_h","id_resp_p"]).size())
    del conn,final



def pd_part2():
    logfile_http = 'resources/http.log'
    http_df = pd.read_csv(logfile_http,
                          sep='\t', header=None,
                          names=['ts', 'uid', 'id_orig_h', 'id_orig_p', 'id_resp_h', 'id_resp_p',
                                 'trans_depth', 'method', 'host', 'uri', 'referrer', 'user_agent',
                                 'request_body_len', 'response_body_len', 'status_code', 'status_msg',
                                 'info_code', 'info_msg', 'filename', 'tags', 'username',
                                 'password', 'proxied', 'orig_fuids', 'orig_mime_types', 'resp_fuids',
                                 'resp_mime_types', 'sample'])
    http_df['ts'] = [
        datetime.fromtimestamp(float(date))
        for date in http_df['ts'].values
    ]
    #conn["duration"] = pd.to_numeric(conn["duration"], errors='coerce')
    print("\nPART 2 \n")
    print("\nHTTP connections over not standard ports (80,8080) grouped by Quarter")
    final2 = http_df.loc[ (http_df['id_orig_p'] != 80) & (http_df['id_resp_p'] != 80) & (http_df['id_orig_p'] != 8080) & (http_df['id_resp_p'] != 8080)]
    final3=final2.groupby([final2['ts'].dt.to_period('Q')]).size()

    print(final3)
    import matplotlib.pyplot as plt
    final3.plot()
    plt.show()

    # Duration is in seconds
    #print("\nConnections over 5 seconds Grouped by Year/ Month")
    #conn5seconds = conn.loc[conn['duration'] >= 5]
    #print(conn5seconds.groupby([conn5seconds.ts.dt.year, conn5seconds.ts.dt.month]).size())

    print("exetypes")

    executable_types = set(['application/x-dosexec', 'application/octet-stream', 'binary', 'application/vnd.ms-cab-compressed'])
    common_exploit_types = set(['application/x-java-applet', 'application/pdf', 'application/zip', 'application/jar','application/x-shockwave-flash'])
    exe_comm=http_df[http_df['resp_mime_types'].isin(executable_types) |  http_df['user_agent'].isin(common_exploit_types)]
    exe_comm=exe_comm.groupby([exe_comm['ts'].dt.to_period('Q')]).size()
    print(exe_comm)
    exe_comm.plot()
    plt.show()
    print(final3)

    print("asdasdfasdf")
    usernames=final2.groupby([final2['username']]).size()
    print(usernames)


#pd_part1()
pd_part2()

#shuf -n 2269435 conn-log -o sample.log
