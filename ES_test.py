import json
import pandas as pd
import csv
import collections
import datetime
import time
from elasticsearch import Elasticsearch, helpers
import json
from time import sleep
from datetime import datetime

ES_HOST = 'localhost'  # URL or host (ex.'127.0.0.1')
es_client = Elasticsearch(f"http://{ES_HOST}:9200")

print('++++++++++++++++ElasticSearch host 접속확인 ++++++++++++++++++')
print(es_client.info())
print('+++++++++++++++++++++++++++++++++++++++++++++++++++++')

index_name = "youtube_data"
if es_client.indices.exists(index=index_name):
    es_client.indices.delete(index=index_name)
    es_client.indices.create(index=index_name)
else:
    es_client.indices.create(index=index_name)

start_time = time.time()
csv_data = pd.read_csv("221219_extract_file.csv")
colunms = [col for col in csv_data]


to_es_data = []

for idx, row in csv_data.iterrows():

    # 1행 doc생성
    row_list = []
    for i in range(len(colunms)):
        row_data = [colunms[i], row[i]]
        row_list.append(row_data)

    doc = [{
            "_index": index_name,
            "_type": index_name, 
            "_id": idx,
            "_source": dict(row_list) # 각 행 dictionary 추가함
            }]
    to_es_data.append(doc)
end_time = time.time()
print(f"{len(to_es_data)} data done. time taken ===> {end_time - start_time:.5f}sec")


helpers.bulk(client = es_client, actions = to_es_data)
# (200, [])   -- 200 indexed, no errors.
index_name = "youtube_data"
es_client.indices.refresh(index=index_name)
# Check the results:
result = es_client.count(index=index_name)
print(result.body['count'])
# 200


###### 함수형태로 저장
# def generate_docs():
#     start_time = time.time()
#     data = pd.read_csv("C:/Users/Chaaaaabbo/Desktop/DOTHIS/221219_extract_file.csv")
#     colunms = [col for col in data]

#     for idx, row in data.iterrows():

#         # 1행 doc생성
#         row_list = []
#         for i in range(len(colunms)):
#             row_data = [colunms[i], row[i]]
#             row_list.append(row_data)

#         doc = {
#                 "_index": index_name,
#                 "_id": idx,
#                 "_source": dict(row_list) # 각 행 dictionary 추가함
#                 }
#         yield doc
#     end_time = time.time()
#     print(f"{len(data)} data done. time taken ===> {end_time - start_time:.5f}sec")

# ######## DATA 삽입 ############
# # mapping 읽은 후 데이터 삽입
# def insertData():
#     es_client = Elasticsearch("http://localhost:9200")

#     index_name = "youtube_data"

#     # mapping file load
#     with open('mapping.json', 'r') as f:
#         mapping = json.load(f)

#     if es_client.indices.exists(index=index_name):
# 	    pass
#     else:
#         es_client.indices.create(index=index_name, body=mapping)
    
#     # doc에 data zip
#     docs = []

#     for num in range(100):
#         docs.append({
#             '_index': index_name,
#             '_source': {
#                 "category": "test"
#                 "c_key": "test"
#                 "status": "test"
#                 "price": 1111
#                 "@timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
#                 }
#                 })

#     # bulk insert
#     helpers.bulk(es_client, docs)

######################################################

# # doc 객체 생성
# doc = {
#     'author': 'kimchy',
#     'text': 'Elasticsearch: cool. bonsai cool.',
#     'timestamp': datetime.now(),
# }

# # 인덱스 생성
# resp = client.index(index="test-index", id=1, document=doc)
# print(resp['result'])

# # 인덱스 조회
# resp = client.get(index="test-index", id=1)
# print(resp['_source'])

# client.indices.refresh(index="test-index")

# resp = client.search(index="test-index", query={"match_all": {}})
# print("Got %d Hits:" % resp['hits']['total']['value'])
# for hit in resp['hits']['hits']:
#     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])