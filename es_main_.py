
"""Script that downloads a public dataset and streams it to an Elasticsearch cluster"""

import csv
from os.path import abspath, join, dirname, exists
import tqdm
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

DATASET_PATH = "221219_extract_file.csv"


def download_dataset():
    with open(DATASET_PATH, 'r', encoding="UTF-8") as f:
        return sum([1 for _ in f]) - 1


def create_index(client, index_name):
    """Creates an index in Elasticsearch. 
    if one is already there, we make new index after delete that."""
    
    # elasticsearch-index-configurations -> 추후 .py 파일로 빼야함
    # https://gist.github.com/lynnkwong/3c5ed5b3225a1e4e56e9bc6b739881e2#file-elasticsearch-index-configurations-py
    configurations = {
        "settings": {
            "index": {"number_of_replicas": 1}, # 기존 2
            # "analysis": {       <- 여기 검색관련 분야인데 더 찾아봐야함 
            #     "filter": {
            #         "ngram_filter": {
            #             "type": "edge_ngram",
            #             "min_gram": 2,
            #             "max_gram": 15,
            #         },
            #     },
            #     "analyzer": {
            #         "ngram_analyzer": {
            #             "type": "custom",
            #             "tokenizer": "standard",
            #             "filter": ["lowercase", "ngram_filter"],
            #         },
            #     },
            # },
        },
        "mappings": {
            "properties" : {
                "TITLE" : {"type" : "text"},
                "URL" : {"type" : "text"},
                "DESCRIPTION" : {"type" : "text"},
                "DURATION" : {"type" : "integer"},
                "PUBLISHED" : {"type" : "date","format" : "yyyy-MM-dd"},
                "VIEWS" : {"type" : "integer"},
                "LIKES" : {"type" : "integer"},
                "TAGS" : {"type" : "text"},
                "CATEGORY" : {"type" : "text"},
                "CHANNEL" : {"type" : "text"},
                "TEXTS" : {"type" : "text"},
                "HASHTAGS" : {"type" : "text"},
                "USE_TEXTS" : {"type" : "text"},
                "GENSIM" : {"type" : "text"},
                "KEYBERT" : {"type" : "text"},
                "AFTER_REMOVE_ADVERB" : {"type" : "text"}
            }
        },
    }
    client.options(ignore_status=404).indices.delete(index=index_name)
    client.indices.create(
        index=index_name,
        settings=configurations["settings"],
        mappings=configurations["mappings"],
    )


def generate_actions():
    """Reads the file through csv.DictReader() and for each row
    yields a single document. This function is passed into the bulk()
    helper to create many documents in sequence.
    """
    with open(DATASET_PATH, mode="r",encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for idx, row in enumerate(reader):
            doc = {
                "_id": idx,
                "TITLE" : row["TITLE"],
                "URL" : row["URL"],
                "DESCRIPTION" : row["DESCRIPTION"],
                "DURATION" : row["DURATION"],
                "PUBLISHED" : row["PUBLISHED"],
                "VIEWS" : row["VIEWS"],
                "LIKES" : row["LIKES"],
                "TAGS" : row["TAGS"],
                "CATEGORY" : row["CATEGORY"],
                "CHANNEL" : row["CHANNEL"],
                "TEXTS" : row["TEXTS"],
                "HASHTAGS" : row["HASHTAGS"],
                "USE_TEXTS" : row["USE_TEXTS"],
                "GENSIM" : row["GENSIM"],
                "KEYBERT" : row["KEYBERT"],
                "AFTER_REMOVE_ADVERB" : row["AFTER_REMOVE_ADVERB"]
            }
            yield doc


def main():
    print("Loading data :)")
    number_of_docs = download_dataset()

    es_host = 'localhost'  # URL or host (ex.'127.0.0.1')
    client = Elasticsearch(f"http://{es_host}:9200")

    print("Creating an index :)")
    index_name = "youtube_data"
    create_index(client, index_name)

    print("=============== Indexing documents ===============")
    progress = tqdm.tqdm(unit="docs", total=number_of_docs)
    successes = 0
    for ok, action in streaming_bulk(
        client=client, index=index_name, actions=generate_actions(),
    ):
        progress.update(1)
        successes += ok
    print("Indexed %d/%d documents" % (successes, number_of_docs))


if __name__ == "__main__":
    main()