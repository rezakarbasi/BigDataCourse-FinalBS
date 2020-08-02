# KAFKA GUIDE

# # Start ZooKeeper Server
# bin/zookeeper-server-start.sh config/zookeeper.properties

# # Start Kafka Server
# bin/kafka-server-start.sh config/server.properties

# # Create topic command
# bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sample

# # Consumer command
# bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning



from kafka import KafkaProducer
import json

import sys
sys.path.append('/media/arzkarbasi/DataDrive/PersonalFiles/Projects/1_DarCProj/Big Data/final project/BigDataCourse-FinalBS/crawlers/soroush_crawler/online crawler')
from seleniumCrawler import SeleniumSoroushCrawler


class SoroushCrawlerObject:
    def __init__(self, channelsList, kafkaTopic, kafkaPort=9092, crawlerType='selenium'):
        if 'selenium' in crawlerType.lower():
            self.crawler = SeleniumSoroushCrawler

        self.channelList = []
        self.AddChannel(channelsList)
        self.kafkaTopic = kafkaTopic

        # kafka init
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:'+str(kafkaPort),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        # self.producer = KafkaProducer(bootstrap_servers='localhost:9092')

    def Run(self):
        out = {}
        out['total'] = 0
        out['messages'] = []
        for data in self.crawler(self.channelList):
            out['messages'].append(data)
            out['total'] = len(out['messages'])
            
            self.producer.send(self.kafkaTopic,out)
            out['messages'] = []

    def AddChannel(self,newChannels):
        if type(newChannels) != list :
            newChannels = list(newChannels)

        for i in newChannels:
            if not(i in self.channelList):
                self.channelList.append(i)

    def RemoveChannel(self,removeChanels):
        if type(removeChanels) != list :
            removeChanels = list(removeChanels)
            
        for i in removeChanels:
            if (i in self.channelList):
                self.channelList.remove(i)
