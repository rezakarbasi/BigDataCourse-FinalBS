import json
import re
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--stopwords_file', default='persian_stopwords.txt', type=str, help='Persian stopwords')
    parser.add_argument('--setting_file', default='setting.json', type=str, help='Setting JSON file')
    args = parser.parse_args()

    stopwords = []
    with open(args.stopwords_file, 'r') as f:
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

    with open(args.setting_file, 'w') as f:
        f.close()

    with open(args.setting_file, 'w') as f:
        f.write(json.dumps(setting))
