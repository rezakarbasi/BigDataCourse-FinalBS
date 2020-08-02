import sys
sys.path.append('/media/arzkarbasi/DataDrive/PersonalFiles/Projects/1_DarCProj/Big Data/final project/BigDataCourse-FinalBS/crawlers/soroush_crawler')
from SoroushCrawler import SoroushCrawlerObject
import json

channelLists = ['bourseprofile']#,'boursnema' , 'bourseabad','boursetraining','farachart_signal','boursecodal','Outbears','bourse0ta1000','bourselaws','boursecode','boursetraining','BourseProfile','boursefaa']

sc = SoroushCrawlerObject(channelLists)
sc.MakeEmptyDataFile()
sc.Run()
sc.SaveTheData()
# print(sc.pastData)