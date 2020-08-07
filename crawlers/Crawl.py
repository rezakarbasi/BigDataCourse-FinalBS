import sys
sys.path.append('/media/arzkarbasi/DataDrive/PersonalFiles/Projects/1_DarCProj/Big Data/final project/BigDataCourse-FinalBS/crawlers/soroush_crawler')
from SoroushCrawler import SoroushCrawlerObject
import json
import threading
import time

channelLists = ['bourseprofile','boursnema' , 'bourseabad','boursetraining','farachart_signal','boursecodal','Outbears','bourse0ta1000','bourselaws','boursecode','boursetraining','BourseProfile','boursefaa']

topicName = 'sample'

sc = SoroushCrawlerObject(channelLists,topicName)

# arbitary send past data
th2 = threading.Thread(target=sc.SendPastData,args=(5,))
th2.start()

time.sleep(5)

# for streaming data
th3 = threading.Thread(target=sc.Run,args=(False,600,))
th3.start()

# add this section at end of code
sc.SaveTheData()
