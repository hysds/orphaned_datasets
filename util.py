import json
import requests
from datetime import datetime
import pytz
import csv
from botocore.exceptions import ClientError


class OrphanedDatasetsFinder:
    def __init__(self, mode, s3_client, bucket_resource, bucket, all_datasets_in_grq, results_file, logger):
        self.counter = 0

        self.mode = mode  # ['PRETEND', 'PERMANENT']
        self.s3_client = s3_client  # boto3.client('s3')
        self.bucket_resource = bucket_resource  # boto3.resource('s3').Bucket(bucket).Bucket(bucket)
        self.bucket = bucket  # bucket name
        self.all_datasets_in_grq = all_datasets_in_grq  # set of (id, version)
        self.end_level = 6  # int (this is how deep in s3 we track the data_set_id as directory)

        self.logger = logger  # logger object

        self.results_file = results_file  # open(FILE_NAME, 'w')
        self.csv_writer = csv.writer(self.results_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        self.csv_writer.writerow(['bucket', 'key', 'data_set_id', 'purge_reason'])

    def traverse_s3_bucket(self, prefix, level):
        '''
        :param prefix: str (ex. aria-ops-dataset-bucket/datasets/interferogram/v2.0.1/2015/06/05/S1-GUNW.../
        :param level: int: how deep we crawl into our s3 bucket
        :return: void
        '''
        result = self.s3_client.list_objects(Bucket=self.bucket, Prefix=prefix, Delimiter='/')
        client_results = result.get('CommonPrefixes')

        if not client_results and level < self.end_level:  # edge case so nothing is accidentally deleted
            self.logger.info('Invalid S3 path: doesnt match convention dataset/version/YYYY/MM/DD/dataset/ %s' % prefix)
            return

        if not client_results or level == self.end_level:  # base case
            self.counter += 1
            if self.counter % 50 == 0:
                print(self.counter)

            split_prefix = prefix.split('/')  # splitting prefix paths
            data_set_id = split_prefix[-2]
            version = split_prefix[2]  # also want to keep the dataset version consistent
            data_set = (data_set_id, version)  # to compare against the GRQ results (dataset_id, version)

            if data_set in self.all_datasets_in_grq:
                self.logger.info('%i %s %s found in GRQ' % (self.counter, data_set_id, version))
            else:
                self.logger.error('%i %s %s NOT FOUND IN GRQ' % (self.counter, data_set_id, version))
                data_set_json = prefix + data_set_id + '.dataset.json'  # valid data sets have this file in s3
                data_json_last_modified = check_file_date_s3(self.s3_client, self.bucket, data_set_json)

                if not data_json_last_modified:  # .dataset.json not found in s3, therefore it is not a valid dataset
                    self.logger.error('%s NOT FOUND IN S3' % data_set_json)
                    self.csv_writer.writerow([self.bucket, prefix, data_set_id, 'dataset.json NOT FOUND IN S3'])
                    if self.mode == 'PERMANENT':  # purge data_set_id
                        self.logger.info('Sent to purge: %s' % prefix)
                        delete_s3_folder(self.bucket_resource, prefix)
                    else:
                        self.logger.info('Not sent to purge. Add -f flag to purge dataset')
                else:
                    self.logger.info('%s FOUND IN S3' % data_set_json)
                    # check how old the .dataset.json file is, if its older than 24 hours, then purge
                    now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)

                    if (now_utc - data_json_last_modified).total_seconds() > 86400:  # if greater than 24 hours
                        self.logger.info('%s IS OLDER THAN 24 HOURS, WILL DELETE' % data_set_json)
                        self.csv_writer.writerow([self.bucket, prefix, data_set_id, 'dataset.json FILE > 24 HOURS'])
                        if self.mode == 'PERMANENT':
                            self.logger.info('Sent to purge: %s' % prefix)
                            delete_s3_folder(self.bucket_resource, prefix)
                        else:
                            self.logger.info('Not sent to purge. Add -f flag to purge dataset')
                    else:
                        self.logger.info('%s is NOT OLDER than 1 day, will not delete' % data_set_json)
            self.results_file.flush()  # in case anything breaks we still have a audit file of what was deleted
            return

        for o in client_results:  # recursive step
            next_prefix = o.get('Prefix')
            self.traverse_s3_bucket(next_prefix, level + 1)  # recurse on each 'sub-directory' in s3


def pull_all_data_sets(es_url, data_type):
    batch_size = 1000

    def pull_es_data(start):
        """
        :param start: integer
        :return: [(SLC_IW, v1.1), ...]
        """
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
            raise BaseException("ElasticSearch went wrong")
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
            raise BaseException("Something with S3 went wrong, exiting job")


def delete_s3_folder(bucket, prefix):
    bucket.objects.filter(Prefix=prefix).delete()


def publish_dataset(product_id):
    '''
    generates the appropriate product json files in the product directory
    '''
    dataset_json_file = '%s/%s.dataset.json' % (product_id, product_id)
    dataset_json = {
        'label': product_id,
        'version': 'v1.0'
    }
    with open(dataset_json_file, 'w') as f:
        json.dump(dataset_json, f, indent=2, sort_keys=True)

    met_json_file = '%s/%s.met.json' % (product_id, product_id)
    with open(met_json_file, 'w') as f:
        json.dump({}, f)


def read_context():
    '''
    reads contents of _context.json file
    :return: json object
    '''
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except Exception as e:
        raise Exception('unable to parse _context.json from work directory')
