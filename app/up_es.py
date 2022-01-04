import json
import requests, json, os
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': '9200'}])

data = [line for line in open('./data/demo.json', 'r', encoding='utf-8')]
print(data)

for i, new in enumerate(data):
    es.index(index='comments', doc_type='docket', id=i, document=json.dumps(json.loads(new),ensure_ascii = False))
    

