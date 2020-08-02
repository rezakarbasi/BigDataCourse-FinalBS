import json
import hashlib 

import sys
sys.path.append('/media/arzkarbasi/DataDrive/PersonalFiles/Projects/1_DarCProj/Big Data/final project/BigDataCourse-FinalBS/crawlers')
from functions import GetDataFromField , GetHashtags

path = './crawlers/soroush_crawler/DataSet.json'
appendPath = './crawlers/soroush_crawler/logged.json'

with open(appendPath , 'r') as f:
    data = json.load(f)

out={'total':len(data),'messages':[]}

for d in data['messages'] :
    o={}

    o['channel_id'] = d['channel_id']
    o['text'] = d['emptytext']
    o['type'] = d['template'].upper()
    o['timestampISO'] = d['timestampISO']
    
    tagText='sapp/'+o['channel_id']+'/'+o['timestampISO']
    o['message_id'] = hashlib.md5(tagText.encode()).hexdigest()
    o['id'] = o['message_id']

    o['hashtags'] = GetHashtags(d['text'])
    o['keywords'] = d['frequency_word']

    out['messages'].append(o)
    out['total'] += 1

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

out['total'] = len(out['messages'])
preData['total'] = len(preData['messages'])

with open(path , 'w') as f:
    # json.dump(out,f)
    json.dump(preData,f)

print(str(out['total']) , ' data added to dataset file.')
