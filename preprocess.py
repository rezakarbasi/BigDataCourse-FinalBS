import json
import re

src_file = 'test_crawl.json'
tgt_file = 'tgt_crawl.json'
stopwords_file = 'persian_stopwords.txt'
setting_file = 'setting.json'

stopwords = []
with open(stopwords_file, 'r') as f:
    for line in f:
        stopwords.append(line.strip())
char_mapping = [
    "\\u200C=>\\u0020",
    "٠=>0",
    "۱=>1",
    "۲=>2",
    "۳=>3",
    "۴=>4",
    "۵=>5",
    "۶=>6",
    "۷=>7",
    "۸=>8",
    "۹=>9",
    "٤=>4",
    "٥=>5",
    "٦=>6",
    ".=>.",
    "،=>,",
    "؟=>?",
    "!=>!",
    "آ=>ا",
    ":)=>_خوشحال_",
    ":(=>_ناراحت_"
]
setting = {
    "settings": {
        "analysis": {
          "char_filter": {
            "zero_width_spaces": {
                "type": "mapping",
                "mappings": char_mapping
            }
          },
          "filter": {
            "persian_stop": {
              "type": "stop",
              "stopwords":  stopwords
            }
          },
          "analyzer": {
            "rebuilt_persian": {
              "tokenizer": "standard",
              "char_filter": ["zero_width_spaces"],
              "filter": [
                "lowercase",
                "decimal_digit",
                "arabic_normalization",
                "persian_normalization",
                "persian_stop"
              ]
            }
          }
        }
    }
}

with open(setting_file, 'w') as f:
    f.close()

with open(setting_file, 'w') as f:
    f.write(json.dumps(setting))

with open(src_file, 'r') as f:
    data = json.load(f)

with open(tgt_file, 'w') as f:
    f.close()

with open(tgt_file, 'w') as f:
    message_id = 1
    for sample_txt in data['messages']:
        cmd_text = '{"index": {"_id": "'+str(message_id)+'"}}\n'
        f.write(cmd_text)
        f.write(json.dumps(sample_txt)+'\n')
        message_id += 1