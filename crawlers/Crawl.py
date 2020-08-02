import sys
sys.path.append('/media/arzkarbasi/DataDrive/PersonalFiles/Projects/1_DarCProj/Big Data/final project/BigDataCourse-FinalBS/crawlers/soroush_crawler')
from SoroushCrawler import SoroushCrawlerObject
import json
import threading
import time

channelLists = ['bourseprofile','boursnema' , 'bourseabad','boursetraining','farachart_signal','boursecodal','Outbears','bourse0ta1000','bourselaws','boursecode','boursetraining','BourseProfile','boursefaa']

sc = SoroushCrawlerObject(channelLists)

# arbitary
dataSetPath = '/media/arzkarbasi/DataDrive/PersonalFiles/Projects/1_DarCProj/Big Data/final project/BigDataCourse-FinalBS/crawlers/soroush_crawler/offline crawler/DataSet.json'
th1 = threading.Thread(target=sc.SendFile,args=(dataSetPath,False,5,))
th1.start()

# for streaming data
th1 = threading.Thread(target=sc.Run,args=(dataSetPath,False,5,))
th1.start()

# add this section at end of code
sc.SaveTheData()
