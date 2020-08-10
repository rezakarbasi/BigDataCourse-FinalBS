from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy, ConsistencyLevel
from cassandra.query import tuple_factory
import json
import uuid
from kafka import KafkaConsumer
from json import loads
def insert_data(data, session):
    time_index = data['timestampISO']
    message_id = data['message_id']
    channel_id = data['channel_id']
    hashtags = data['hashtags']
    requete = "insert into total_posts (time_index, message_id) values ('%s','%s')" % (time_index, message_id)
    session.execute(requete)
    ##get tabel names
    o = session.execute('''SELECT * FROM system_schema.tables WHERE keyspace_name = 'mykeyspace' ''')
    flag = True
    for i in o:
        if i[1] == channel_id or i[1] == channel_id.lower():
            flag = False
    ##create channel if does not exist
    if flag == True:
        qry = '''create table ''' + channel_id + ''' (
           time_index Timestamp,
           message_id text,
           primary key(time_index)
        );'''
        session.execute(qry)
    ##inser channel data
    qry = "insert into " + channel_id + " (time_index, message_id) values ('%s','%s')" % (time_index, message_id)
    session.execute(qry)
    ### creating hashtag tabel
    qry = "SELECT * FROM hashtags"
    o = session.execute(qry)
    hashtag_names = []
    hashtag_id = []
    for i in o:
        hashtag_names.append(i[1])
        hashtag_id.append(str(i[0]))
    for i in hashtags:
        if i not in hashtag_names:
            uid = str(uuid.uuid1())
            qry = "insert into hashtags (hashtag_id, hashtag) values (%s,'%s')" % (uid, i)
            session.execute(qry)
            id = uid.split('-')
            tabel_name = "hashtag"
            for j in id:
                tabel_name = tabel_name + '_' + j
            qry = '''create table ''' + tabel_name + ''' (
               time_index Timestamp,
               message_id text,
               primary key(time_index)
            );'''
            session.execute(qry)
        else:
            id = hashtag_id[hashtags.index(i)].split('-')
            tabel_name = "hashtag"
            for j in id:
                tabel_name = tabel_name + '_' + j
        qry = "insert into " + tabel_name + " (time_index, message_id) values ('%s','%s')" % (time_index, message_id)
        session.execute(qry)


try:
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
    consumer = KafkaConsumer(
        'sample01',
         bootstrap_servers=['localhost:9092'],
         auto_offset_reset='earliest',
         enable_auto_commit=True,
         group_id='my-group',
         value_deserializer=lambda x: loads(x.decode('utf-8')))
    for message in consumer:
        print("ok")
        message = message.value
        insert_data(message, session)
except KeyboardInterrupt:
    pass



