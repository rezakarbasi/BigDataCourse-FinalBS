# web driver must bokhore be google chrome
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
# import requests as re
import time
import hashlib
import json

import sys
sys.path.append('/media/arzkarbasi/DataDrive/PersonalFiles/Projects/1_DarCProj/Big Data/final project/BigDataCourse-FinalBS/crawlers')
from functions import GetDataFromField , GetHashtags


def SeleniumDecoder(postSource,postText,channelName):

    d=postSource
    o={}

    o['channel_id'] = channelName

    b , typee = GetDataFromField(d,['@type'])
    if b :
        if 'video' in typee.lower():
            o['type']='VIDEO'
        else :
            b1 , imUrl = GetDataFromField(d,['image'])
            imUrl = imUrl[0]
            b2 , logoUrl = GetDataFromField(d,['publisher','logo','url'])

            if b1 and b2 :
                if imUrl == logoUrl :
                    o['type']='TEXT'
                else :
                    o['type']='IMAGE'
            else :
                o['type']='error 2'
                print('type error 2')
    else :
        o['type']='error 1'
        print('type error 1')
    
    b,h = GetDataFromField(d,'headline')
    if b :
        o['title']=h
    else :
        print('headline error')

    b,t = GetDataFromField(d,'description')
    if b :
        o['text']=t
    else :
        print('description error')
    
    b,pub = GetDataFromField(d,'datePublished')
    if not(b) :
        b,pub = GetDataFromField(d,'uploadDate')
        if not(b):
            pub=''
            print('error publish date')
    o['timestampISO']=pub

    tagText='sapp/'+o['channel_id']+'/'+o['timestampISO']
    o['message_id'] = hashlib.md5(tagText.encode()).hexdigest()
    o['id'] = o['message_id']

    o['hashtags'] = GetHashtags(postText)
    o['keywords'] = []    

    return o



def SeleniumSoroushCrawler(channelLists=['bourseabad']):
    
    driver = webdriver.Chrome('/home/arzkarbasi/Downloads/chromedriver_linux64/chromedriver')

    baseUrl = 'https://what.sapp.ir/'
    startString = '<script type="application/ld+json">'
    endString = '</script>'

    for i in channelLists :
        url = baseUrl+i

        driver.get(url)
        time.sleep(3)

        p = driver.page_source

        idx = p.find(startString)

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

                yield SeleniumDecoder(json.loads(post),text,i)

            idx = p.find(startString)


    driver.close()
