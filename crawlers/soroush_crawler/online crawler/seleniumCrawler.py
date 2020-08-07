# web driver must bokhore be google chrome
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
# import requests as re
import time
import hashlib
import json
from hazm import Normalizer

import sys
sys.path.append('/media/arzkarbasi/DataDrive/PersonalFiles/Projects/1_DarCProj/Big Data/final project/BigDataCourse-FinalBS/crawlers')
from functions import GetDataFromField , GetHashtags , GetLinks , FindWords , ListToString


def SeleniumDecoder(postSource,postText,channelName,syms,keywords):

    d=postSource
    o={}

    o['channel_id'] = channelName

    b , typee = GetDataFromField(d,['@type'])
    b1 , imUrl = GetDataFromField(d,['image'])
    if b :
        if 'video' in typee.lower():
            o['type']='VIDEO'
        else :
            b2 , logoUrl = GetDataFromField(d,['publisher','logo','url'])

            if b1 and b2 :
                if imUrl[0] == logoUrl :
                    o['type']='TEXT'
                else :
                    o['type']='IMAGE'
            else :
                o['type']='error 2'
                print('type error 2')
    else :
        o['type']='error 1'
        print('type error 1')
    
    o['image'] = []
    if b1:
        o['image'] = imUrl

    b,h = GetDataFromField(d,'headline')
    if b :
        o['title']=h
    else :
        print('headline error')

    b,t = GetDataFromField(d,'description')
    if b :
        normalizer = Normalizer()
        t = normalizer.normalize(t)
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
    o['keywords'] = FindWords(keywords,o['text'])

    o['links'] = GetLinks(postText) 

    o['symbols'] = FindWords(syms,o['text'])

    # custom fields
    o['ROWKEY'] = o['id']
    o['hashtags_str'] = ListToString(o['hashtags'])
    o['keywords_str'] = ListToString(o['keywords'])
    o['symbols_str'] = ListToString(o['symbols'])

    return o



def SeleniumSoroushCrawler(channelLists,syms,keywords):
    
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

                yield SeleniumDecoder(json.loads(post),text,i,syms,keywords)

            idx = p.find(startString)


    driver.close()
