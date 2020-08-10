#!/bin/sh
python preprocess.py --stopwords_file persian_stopwords.txt --setting_file setting.json
curl -XPOST localhost:9200/messages/_close
curl -s -H "Content-Type: application/x-ndjson" -XPUT localhost:9200/messages/_settings --data-binary @setting.json
curl -XPOST localhost:9200/messages/_open
