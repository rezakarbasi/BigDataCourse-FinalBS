# KAFKA GUIDE

# # Start ZooKeeper Server
# bin/zookeeper-server-start.sh config/zookeeper.properties

# # Start Kafka Server
# bin/kafka-server-start.sh config/server.properties

# # Create topic command
# bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic sample

# # Consumer command
# bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
# import requests as re
import time
import hashlib
import json
from kafka import KafkaProducer

def SeleniumSoroushCrawler(channelLists):

    # kafka init
    # producer = KafkaProducer(bootstrap_servers='localhost:9092')

    driver = webdriver.Chrome('/home/arzkarbasi/Downloads/chromedriver_linux64/chromedriver')

    # channelLists = ['boursnema','bourseabad','boursetraining','bourseprofile','farachart_signal','boursecodal','Outbears','bourse0ta1000','bourselaws','boursecode','boursetraining','BourseProfile','boursefaa']

    baseUrl = 'https://what.sapp.ir/'
    startString = '<script type="application/ld+json">'
    endString = '</script>'
    out = []

    for i in channelLists :
        # print(len(out))
        url = baseUrl+i

        driver.get(url)
        time.sleep(1)

        p = driver.page_source

        idx = p.find(startString)
        i=0
        while idx != -1 :
            p = p[ idx+len(startString) :]
            endIdx = p.find(endString)

            post = p[:endIdx]

            p=p[endIdx+len(endString):]
            idx1 = p.find('<div')
            if idx1<10 :
                p = p[idx1+4:]
                p = p[p.find('>')+1:]
                text = p[:p.find('</div>')]
                out.append([json.loads(post),text])
                
                # producer.send('sample',b'happy')

            idx = p.find(startString)


    driver.close()
    # print(len(out))

    # with open('a.json','w') as f:
    #     json.dump(out,f)
