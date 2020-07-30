import json
import hashlib 

import sys
sys.path.append('/media/arzkarbasi/DataDrive/PersonalFiles/Projects/1_DarCProj/Big Data/final project/BigDataCourse-FinalBS/crawlers')
from functions import GetDataFromField

path = './crawlers/soroush_crawler/DataSet.json'
appendPath = './crawlers/soroush_crawler/crawlTest.json'

with open(appendPath , 'r') as f:
    data = json.load(f)

out={'total':len(data),'messages':[]}

for d in data :
    o={}

    o['channel_id'] = 'bourseprofile'           ### felan ine . badan bas doros she

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

    o['hashtags'] = []
    o['keywords'] = []

    out['messages'].append(o)

with open(path , 'r') as f:
    preData = json.load(f)

ids = list(map(lambda  z: z['id'],preData['messages']))

for idx in range(len(out['messages'])-1,-1,-1):
    d = out['messages'][idx]
    if d['id'] in ids :
        out['messages'].remove(d)
        out['total'] -= 1
    else :
        preData['messages'].append(d)
        preData['total'] += 1

with open(path , 'w') as f:
    json.dump(preData,f)

print(str(out['total']) , ' data added to dataset file.')
