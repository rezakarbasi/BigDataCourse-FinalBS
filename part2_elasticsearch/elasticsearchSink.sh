# To create Stream in ksqldb
CREATE STREAM MESSAGES (channel_id VARCHAR, type VARCHAR, image ARRAY<VARCHAR>, title VARCHAR,text VARCHAR ,timestampISO VARCHAR,message_id VARCHAR,id VARCHAR,hashtags ARRAY<VARCHAR>,keywords ARRAY<VARCHAR>, links ARRAY<VARCHAR>, symbols ARRAY<VARCHAR>,ROWKEY VARCHAR KEY, hashtags_str VARCHAR,keywords_str VARCHAR, symbols_str VARCHAR) WITH (KAFKA_TOPIC='messages', VALUE_FORMAT='JSON');
# To keep track and see data flow in the stream
SELECT * FROM messages EMIT CHANGES ;
# To create Elasticsearch Sink Connector
CREATE SINK CONNECTOR ELASTICSEARCH_SINK WITH (
'connector.class'         = 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
'connection.url'          = 'http://elasticsearch:9200',
'key.converter'           = 'org.apache.kafka.connect.storage.StringConverter',
'value.converter'         = 'org.apache.kafka.connect.json.JsonConverter',
'value.converter.schemas.enable' = 'false',
'type.name'               = '_doc',
'topics'                  = 'messages',
'key.ignore'              = 'true',
'schema.ignore'           = 'true'
);
