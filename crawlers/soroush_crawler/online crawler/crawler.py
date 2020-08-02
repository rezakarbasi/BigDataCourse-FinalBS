from bs4 import BeautifulSoup
import requests as re
import time
import hashlib
import json

channelLists = ['boursnema']#,'bourseabad','boursetraining','bourseprofile','farachart_signal','boursecodal','Outbears','bourse0ta1000','bourselaws','boursecode','boursetraining','BourseProfile','boursefaa']

baseUrl = 'https://what.sapp.ir/'
css = '/asts-fl/css/cache-css-all.css'
js = '/asts-fl/js/cache-js-all.js'

for i in channelLists :
    # time.sleep(3)

    url = baseUrl+i

    headers = {
        'Upgrade-Insecure-Requests' : '1',
        'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36',
        'Accept' : 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
    }

    p = re.get(url, headers=headers)

    soup = BeautifulSoup(p.content, 'html.parser')
    a=soup.find_all('div',attrs={'class':'page'})
    for post in a[3].findAll('div',attrs={'class':'grid__item'}):
        print(post)

    print('------------------')
    print(i)


# with open('a.txt','w') as f:
#     f.write(soup.prettify())