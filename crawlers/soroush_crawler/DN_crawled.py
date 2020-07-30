import json
import hashlib 

def GetDataFromField(d , field):
    if type(field) != list:
        field=[field]
    o=d

    for f in field:
        if type(o) != dict:
            return False,'ERROR DICT'

        if f in o.keys() :
            o=o[f]
        else :
            return False , o
    return True , o

with open('crawlTest.json' , 'r') as f:
    data = json.load(f)

out={'total':len(data),'messages':[]}


for d in data[:] :
    o={}

    o['channel_id'] = 'bourseprofile'           ### felan ine . badan bas doros she

    b , typee = GetDataFromField(d,['@type'])
    if b :
        if 'video' in typee.lower():
            o['type']='VIDEO'
        else :
            b1 , imUrl = GetDataFromField(d,['mainEntityOfPage','@id'])
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
    o['message_id'] = hashlib.md5(tagText.encode()).digest()
    o['id'] = o['message_id']

    o['hashtags'] = []
    o['keywords'] = []

    out['messages'].append(o)