from bs4 import BeautifulSoup
import requests as re
import time
import hashlib
import json

channelLists = ['boursnema']#,'bourseabad','boursetraining','bourseprofile','farachart_signal','boursecode']

baseUrl = 'https://what.sapp.ir/'


for i in channelLists :
    url = baseUrl+i

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    }

    p = re.get(url, headers=headers)

    soup = BeautifulSoup(p.content, 'html.parser')
    a=soup.find_all('div',attrs={'class':'page'})
    for post in a[3].findAll('div',attrs={'class':'grid__item'}):
        print(post)
    
    time.sleep(5)
