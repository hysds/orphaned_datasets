import sys
import json
import requests
from botocore.exceptions import ClientError


# datasets\/(.+)\/(v.*)\/(\d{4})\/(\d{2})\/(\d{2})\/(.+?)\/(.*)


def pull_all_data_sets(es_url, data_type):
    batch_size = 1000

    def pull_es_data(start):
        '''

        :param start: integer
        :return: [(SLC_IW, v1.1), ...]
        '''
        es_query = {
            "size": batch_size,
            "from": start,
            "fields": ["_id", "version"],
            "query": {
                "bool": {
                    "must": [
                        {"term": {"dataset_type.raw": data_type}}
                    ]
                }
            }
        }
        req = requests.post(es_url, data=json.dumps(es_query), verify=False)
        if req.status_code != 200:
            raise BaseException("Elasticsearch went wrong")
        es_results = req.json()
        return {(
            row["_id"],
            row["fields"]["version"][0] if row.get("fields") else None)
            for row in es_results["hits"]["hits"]}

    data_sets = set()
    pagination = 0
    while True:
        batch = pull_es_data(pagination)
        if len(batch) == 0:
            break
        data_sets = data_sets.union(batch)
        pagination += batch_size
    return data_sets


def check_file_date_s3(s3_client, bucket, key):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['LastModified']
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        else:
            raise "Something with S3 went wrong, exiting job"


def delete_s3_folder(bucket, prefix):
    bucket.objects.filter(Prefix=prefix).delete()


class Counter:
    def __init__(self):
        self.counter = 0
        self.statistics = {}

    def increment(self):
        self.counter += 1

    def get_count(self):
        return self.counter
