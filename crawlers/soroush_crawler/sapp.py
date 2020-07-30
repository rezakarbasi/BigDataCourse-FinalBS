import hashlib
import requests
import json

def get_channel_data(channel_name, size = 20, last_message_id = -1):
    endpoint = 'channel/archive'
    a = "95d58639"
    b = "24c0"
    c = "4b72"
    d = "80a5"
    f = "e124f41d7af9"
    api_secret = "-".join([a,b,c,d,f])
    api_resource = 'channel/archive'
    clinet_id = '7743461522282941752'
    format = 'json'
    api_version = 'v1'
    if last_message_id != -1:
        message = f'{{"channel_id":"{channel_name}", "limit": {size}, "message_id": "{last_message_id}"}}'
    else:
        message = f'{{"channel_id":"{channel_name}", "limit": {size}}}'
    digest = hashlib.md5((api_secret+api_version+format+clinet_id+api_resource+message).encode('utf-8')).hexdigest()
    url = f"https://what.sapp.ir/srvcs-app/v1/json/7743461522282941752/{digest}/channel/archive"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
    }
    print(url)
    rq = requests.post(url, data = message.encode("utf-8"), headers=headers)
    return json.loads(rq.text)

a=get_channel_data("bourseprofile", 2000)

print(a)

with open('logged.json', 'w') as f:
    json.dump(a,f)
