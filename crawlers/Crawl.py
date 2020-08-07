import sys
sys.path.append('/media/arzkarbasi/DataDrive/PersonalFiles/Projects/1_DarCProj/Big Data/final project/BigDataCourse-FinalBS/crawlers/soroush_crawler')
from SoroushCrawler import SoroushCrawlerObject
import json
import threading
import time

channelLists = ['bourseprofile','boursnema' , 'bourseabad','boursetraining','farachart_signal','boursecodal','Outbears','bourse0ta1000','bourselaws','boursecode','boursetraining','BourseProfile','boursefaa']

sc = SoroushCrawlerObject(channelLists,'hossein')

# arbitary send "bourseprofile" channel data
dataSetPath = '/media/arzkarbasi/DataDrive/PersonalFiles/Projects/1_DarCProj/Big Data/final project/BigDataCourse-FinalBS/crawlers/soroush_crawler/offline crawler/DataSet.json'
th1 = threading.Thread(target=sc.SendFile,args=(dataSetPath,False,5,))
th1.start()

# arbitary send past data
th2 = threading.Thread(target=sc.SendPastData,args=(1,))
th2.start()

# for streaming data
th3 = threading.Thread(target=sc.Run,args=(False,600,))
th3.start()

# add this section at end of code
sc.SaveTheData()
