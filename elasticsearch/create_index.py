from elasticsearch import Elasticsearch
from google.cloud import storage
import argparse
import json
import os
from os import path


bucket_name = 'bigdata-winter2022'
project_name = 'gd-sar-stpractice'


def connection(host):
    es = Elasticsearch([{'host': host, 'port': 9200, 'scheme': 'http'}], http_auth=("elastic", "elastic"))
    if es.ping():
        print("Connected to cluster successfully\n")
        print(f'Cluster information: \n {es.info()}')
    else:
        print("Not connected to cluster\n")
    return es


def create_index(elastic, mapping_file, index):
    settings = mapping_file
    try:
        if not elastic.indices.exists(index=index):
            es.indices.create(index=index, ignore=400, body=settings, include_type_name=True)
            print(f"Index {index} has been created")
    except Exception as e:
        print(str(e))
    finally:
        print(f"Index {index} exists")


class SwitchBase:
    def switch(self, case, path):
        if case != 'gcp':
            return self.case_local(path)
        else:
            return self.case_GCP(path)

    def case_local(self, local_path):
        with open(local_path) as f:
            return json.load(f)

    def case_GCP(self, gcs_json_path):
        os.environ.setdefault("GCLOUD_PROJECT", project_name)
        bucket = storage.Client().bucket(bucket_name)
        blob = bucket.blob(gcs_json_path)
        return blob.download_as_string()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host")
    parser.add_argument("--mode")
    parser.add_argument("--mapping_path")
    args = vars(parser.parse_args())
    host = args['host']
    mode = args["mode"]
    path_file = args['mapping_path']
    es = connection(host)
    SW = SwitchBase()
    mapping_file = SW.switch(mode, path_file)
    json_index_name = path.basename(path_file)
    index_name = str(path.splitext(json_index_name)[0])
    print(f"index_name = {index_name}")
    create_index(es, mapping_file, index_name)
