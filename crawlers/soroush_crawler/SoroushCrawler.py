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
import time

class SoroushCrawlerObject:
    def __init__(self, channelsList, kafkaTopic='sample', kafkaPort=9092, crawlerType='selenium', pathData = 'Data.json'):
        if 'selenium' in crawlerType.lower():
            self.crawler = SeleniumSoroushCrawler

        self.pathData = pathData
        self.channelList = []
        self.AddChannel(channelsList)
        self.kafkaTopic = kafkaTopic

        # kafka init
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:'+str(kafkaPort),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')) # utf to unicode
        # self.producer = KafkaProducer(bootstrap_servers='localhost:9092')

        with open(self.pathData,'r') as f:
            self.pastData = json.load(f)
        self.ids = map(lambda  z: z['id'],self.pastData['messages'])

    def SendFile(self,fileName,sendOnce=False,delayTime=2):
        with open(fileName,'r') as f:
            data = json.load(f)
        
        if sendOnce :
            self.producer.send(self.kafkaTopic,data)
        else :
            for d in data['messages'] :
                o = {'total':1,'messages':[d]}
                self.producer.send(self.kafkaTopic,o)
                time.sleep(delayTime)


    def MakeEmptyDataFile(self):
        emptyDict = {'total' : 0 , 'messages' : []}
        with open(self.pathData , 'w') as f:
            json.dump(emptyDict,f)
            self.pastData = emptyDict
        self.ids = map(lambda  z: z['id'],self.pastData['messages'])

    def Run(self,doOnce=False,periodTime=600):
        while True:
            out = {}
            out['total'] = 0
            out['messages'] = []
            for data in self.crawler(self.channelList):
                if not(data['id'] in self.ids):
                    out['messages'].append(data)
            
            out['total'] = len(out['messages'])

            self.producer.send(self.kafkaTopic,out)
            # bedooone in sleep kar nmikard !
            time.sleep(1)
            self.AddToPastData(out)

            time.sleep(periodTime)
            if doOnce:
                break

    def AddToPastData(self , data=dict):
        for i in data['messages']:
            self.pastData['messages'].append(i)
        self.pastData['total'] = len(self.pastData['messages'])
        self.ids = list(map(lambda  z: z['id'],self.pastData['messages']))

    def SaveTheData(self):
        with open(self.pathData,'w') as f:
            json.dump(self.pastData,f)

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
