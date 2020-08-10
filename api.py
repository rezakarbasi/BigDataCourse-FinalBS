from flask import Flask
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy, ConsistencyLevel
from cassandra.query import tuple_factory
import datetime
import json
from flask import request
app = Flask(__name__)

def iso_8601_format(dt):
    """YYYY-MM-DDThh:mm:ssTZD (1997-07-16T19:20:30-03:00)"""

    if dt is None:
        return ""

    fmt_datetime = dt.strftime('%Y-%m-%dT%H:%M:%S')
    tz = dt.utcoffset()
    if tz is None:
        fmt_timezone = "+00:00"
    else:
        fmt_timezone = str.format('{0:+06.2f}', float(tz.total_seconds() / 3600))

    return fmt_datetime + fmt_timezone

@app.route('/help')
def help():
    return 'Hello, World!'

@app.route('/LastOneHoureTotalPost')
def LasOneHoure():
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        retry_policy=DowngradingConsistencyRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=200,
        row_factory=tuple_factory
    )
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect('mykeyspace')
    t_one = iso_8601_format(datetime.datetime.now())
    t_two = iso_8601_format(datetime.datetime.now() - datetime.timedelta(hours=1))
    qry = "SELECT * FROM total_posts WHERE (time_index > '%s') And (time_index < '%s') ALLOW FILTERING" % (t_two, t_one)
    o = session.execute(qry)
    out = []
    for i in o:
        out.append(str(i))
    return json.dumps({"result":out})

@app.route('/Last24HourChannel/')
def Last24HourChannel():
    channelname = request.args.get("channelname")
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        retry_policy=DowngradingConsistencyRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=200,
        row_factory=tuple_factory
    )
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect('mykeyspace')
    t_one = iso_8601_format(datetime.datetime.now())
    t_two = iso_8601_format(datetime.datetime.now() - datetime.timedelta(hours=24))
    o = session.execute('''SELECT * FROM system_schema.tables WHERE keyspace_name = 'mykeyspace' ''')
    flag = False
    for i in o:
        if i[1] == channelname or i[1] == channelname.lower():
            flag = True
            break
    if flag == True:
        qry = "SELECT * FROM %s WHERE (time_index > '%s') And (time_index < '%s') ALLOW FILTERING" % (
        channelname,t_two, t_one)
        o = session.execute(qry)
        out = []
        for i in o:
            out.append(str(i))
        return json.dumps({"result": out})
    else:
        return "this channel does not exist in data"

@app.route('/Hashtag/')
def Hashtag():
    hashtagname = request.args.get("hashtagname")
    first_date = int(request.args.get("first_date"))
    year = int(first_date / 10000)
    month = int(((first_date%10000) - (first_date%100))/100)
    day = first_date % 100
    first_date = datetime.datetime(year,month,day)
    t_one = iso_8601_format(first_date)
    second_date = int(request.args.get("second_date"))
    year = int(second_date / 10000)
    month = int(((second_date%10000) - (second_date%100))/100)
    day = second_date % 100
    second_date = datetime.datetime(year,month,day)
    t_two = iso_8601_format(second_date)
    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
        retry_policy=DowngradingConsistencyRetryPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        request_timeout=200,
        row_factory=tuple_factory
    )
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
    session = cluster.connect('mykeyspace')
    date = request.args.get("date")
    qry = "SELECT * FROM hashtags"
    o = session.execute(qry)
    hashtag_names = []
    hashtag_id = []
    for i in o:
        hashtag_names.append(i[1])
        hashtag_id.append(str(i[0]))
    if hashtagname in hashtag_names:
        id = hashtag_id[hashtag_names.index(hashtagname)]
        tabel_name = "hashtag"
        id = id.split('-')
        for j in id:
            tabel_name = tabel_name + '_' + j
        qry = "SELECT * FROM %s WHERE (time_index > '%s') And (time_index < '%s')ALLOW FILTERING" % (tabel_name,t_one, t_two)
        o = session.execute(qry)
        out = []
        for i in o:
            out.append(str(i))
        return json.dumps({"result": out})
    return "ok"