"""
Created on Fri Aug  7 18:51:47 2020

@author: hossein
"""

from ksql import KSQLAPI
from clickhouse_driver import Client


if __name__ == '__main__':
    
    # Connect to Kafka SQL SERVER
    client = KSQLAPI('http://localhost:8088')
    
    #-------------------------------------------------------------------------
    #               Create a Json Stream based on exist topic
    #-------------------------------------------------------------------------
    client.create_stream(table_name = 'channels', 
                         columns_type = ['channel_id VARCHAR',
                                         'type VARCHAR',
                                         'image ARRAY<VARCHAR>',
                                         'title VARCHAR',
                                         'text VARCHAR',
                                         'timestampISO VARCHAR',
                                         'message_id VARCHAR',
                                         'id VARCHAR',
                                         'hashtags ARRAY<VARCHAR>',
                                         'keywords ARRAY<VARCHAR>',
                                         'links ARRAY<VARCHAR>',
                                         'symbols ARRAY<VARCHAR>',
                                         'hashtags_str VARCHAR',
                                         'keywords_str VARCHAR',
                                         'symbols_str VARCHAR'], 
                         topic = 'source_topic',
                         value_format = 'JSON')
    
    #-------------------------------------------------------------------------
    # Create a new AVRO Stream based on Previous Stream and Select Some of Columns
    #-------------------------------------------------------------------------
    client.create_stream_as(table_name = 'source_avro',
                            select_columns = ['channel_id',
                                              'type',
                                              'timestampISO',
                                              'message_id',
                                              'id',
                                              'hashtags_str',
                                              'keywords_str',
                                              'symbols_str'],
                            src_table = 'channels',
                            value_format = 'AVRO')
    
    
    
    #-------------------------------------------------------------------------
    #          Connect to ClickHouse and Create new Database 'Channel'
    #-------------------------------------------------------------------------
    clickhouse_client = Client('localhost', port=9000)
    clickhouse_client.execute('DROP DATABASE IF EXISTS Channel')
    clickhouse_client.execute('CREATE DATABASE Channel')
    
    
    #-------------------------------------------------------------------------
    #          Create Table data in ClickHouse to Save Data
    #-------------------------------------------------------------------------
    clickhouse_client.execute('''CREATE TABLE IF NOT EXISTS Channel.data (
                                    CHANNEL_ID Nullable(String),
                                    TYPE Nullable(String),
                                    TIMESTAMPISO String,
                                    TIME_TEMP String DEFAULT substring(TIMESTAMPISO, 1, 19),
                                    TIME DateTime64 DEFAULT toDateTime64(TIME_TEMP, 3),
                                    date Date DEFAULT toDate(TIME),
                                    myTime UInt64 DEFAULT toYYYYMMDDhhmmss(TIME),
                                    MESSAGE_ID Nullable(String),
                                    ID Nullable(String),
                                    HASHTAGS_STR String,
                                    hashtags Array(String) DEFAULT splitByChar(',', HASHTAGS_STR),
                                    hashtags_len UInt8 DEFAULT length(hashtags),
                                    KEYWORDS_STR String,
                                    keywords Array(Nullable(String)) DEFAULT splitByChar(',', KEYWORDS_STR),
                                    keywords_len UInt8 DEFAULT length(keywords),
                                    SYMBOLS_STR String,
                                    symbols Array(String) DEFAULT splitByChar(',', SYMBOLS_STR),
                                    symbols_len UInt8 DEFAULT length(symbols)) ENGINE = MergeTree 
                                                                               PARTITION BY toYYYYMMDD(TIME) 
                                                                               ORDER BY toYYYYMMDD(TIME)''')
    
    
    #-------------------------------------------------------------------------
    #      Create a JDBC Sink Connection for read data from Kafka Stream 
    #-------------------------------------------------------------------------
    client.ksql('''CREATE SINK CONNECTOR `clickhouse-jdbc-connector` 
                   WITH (
                       'connector.class'='io.confluent.connect.jdbc.JdbcSinkConnector',  
                       'topics'='source_avro',
                       'tasks.max'='1',
                       'connection.url'='jdbc:clickhouse://clickhouse:8123/Channel',
                       'table.name.format'='data');''')
