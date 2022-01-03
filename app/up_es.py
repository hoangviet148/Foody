import json
data = [line for line in open('/mnt/c/Users/nnvvh/Downloads/comment_312022.csv', 'r', encoding='utf-8')]

# for new in data[0:2]:
#     print(json.dumps(new,ensure_ascii = False))

import requests, json, os
from elasticsearch import Elasticsearch

res = requests.get('http://172.23.0.2:9200')
print (res.content)
es = Elasticsearch([{'host': '172.23.0.2', 'port': '9200'}])

i=0
for new in data:
    es.index(index='mdth_news_data', doc_type='docket', id=i, document=json.dumps(json.loads(new),ensure_ascii = False))
    i+=1
