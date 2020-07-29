#!/bin/sh
curl -s -H "Content-Type: application/x-ndjson" -XPUT localhost:9200/messages --data-binary @setting.json
curl -s -H "Content-Type: application/x-ndjson" -XPOST localhost:9200/messages/message/_bulk --data-binary @tgt_crawl.json
