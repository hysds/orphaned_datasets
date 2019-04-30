import os
import sys
import json
import re
import boto3
import urllib3
from datetime import datetime
import logging as logger
import csv
import pytz
from util import Counter, pull_all_data_sets, check_file_date_s3, delete_s3_folder

urllib3.disable_warnings()

# TODO: ADD IMPORT CELERY CONFIGS FOR THE ELASTICSEARCH URL AND S3 KEYS
# python purge_orphaned_datasets.py https://c-datasets.aria.hysds.io s3://aria-ops-dataset-bucket/datasets/slc


def traverse_s3_bucket(prefix, level):
    '''
    :param prefix: str (ex. aria-ops-dataset-bucket/datasets/interferogram/v2.0.1/2015/06/05/S1-GUNW.../
    :param level: int: how deep we crawl into our s3 bucket
    :variables:
        bucket = bucket name
        END_LEVEL = furthest level we will travel down a s3 bucket
        s3_client = s3 client API, used to get the common prefix
        s3_resource = used to purge the s3 bucket
        logger = log handler
        csv_writer = csv handler
    :return: void
    '''

    result = s3_client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/')
    client_results = result.get('CommonPrefixes')

    if not client_results and level < END_LEVEL:  # edge case so nothing is accidentally deleted
        logger.info('Invalid S3 path: does not match convention dataset/version/YYYY/MM/DD/dataset/ %s' % prefix)
        return

    if not client_results or level == END_LEVEL:  # base case
        counter.increment()
        if counter.get_count() % 50 == 0:
            print(counter.get_count())

        split_prefix = prefix.split('/')  # splitting prefix paths
        data_set_id = split_prefix[-2]
        version = split_prefix[2]  # also want to keep the dataset version consistent
        data_set = (data_set_id, version)  # to compare against the GRQ results (dataset_id, version)

        if data_set in all_data_sets_in_grq:
            logger.info('%i %s %s found in GRQ' % (counter.get_count(), data_set_id, version))
        else:
            logger.error('%i %s %s NOT FOUND IN GRQ' % (counter.get_count(), data_set_id, version))
            data_set_json = prefix + data_set_id + '.dataset.json'  # valid data sets have this file in s3
            data_json_last_modified = check_file_date_s3(s3_client, bucket, data_set_json)

            if not data_json_last_modified:  # .dataset.json not found in s3, therefore it is not a valid dataset
                logger.error('%s NOT FOUND IN S3' % data_set_json)
                csv_writer.writerow([bucket, prefix, data_set_id, 'dataset.json NOT FOUND IN S3'])
                if PROD_MODE:  # purge data_set_id
                    logger.info('Sent to purge: %s' % prefix)
                    delete_s3_folder(bucket_resource, prefix)
                else:
                    logger.info('Not sent to purge. Add -f flag to purge dataset')
            else:
                logger.info('%s FOUND IN S3' % data_set_json)
                # check how old the .dataset.json file is, if its older than 24 hours, then purge
                now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)

                if (now_utc - data_json_last_modified).total_seconds() > 86400:  # if greater than 24 hours
                    logger.info('%s IS OLDER THAN 24 HOURS, WILL DELETE' % data_set_json)
                    csv_writer.writerow([bucket, prefix, data_set_id, 'dataset.json FILE IS OLDER THAN 24 HOURS'])
                    if PROD_MODE:
                        logger.info('Sent to purge: %s' % prefix)
                        delete_s3_folder(bucket_resource, prefix)
                    else:
                        logger.info('Not sent to purge. Add -f flag to purge dataset')
                else:
                    logger.info('%s is NOT OLDER than 1 day, will not delete' % data_set_json)
        results_file.flush()  # in case anything breaks we still have a audit file of what was deleted
        return

    for o in client_results:  # recursive step
        next_prefix = o.get('Prefix')
        traverse_s3_bucket(next_prefix, level + 1)  # recurse on each 'sub-directory' in s3


def publish_dataset(product_id):
    '''
    generates the appropriate product json files in the product directory
    TODO: edit mozart's datasets.json and add entry with regex and that stuff
    '''
    dataset_json_file = '%s.dataset.json' % product_id
    dataset_json = {
        'label': product_id,
        'version': 'v1.0'
    }
    met_json_file = '%s.met.json' % product_id

    with open(dataset_json_file, 'w') as f:
        json.dump(dataset_json, f, indent=2, sort_keys=True)
    with open(met_json_file, 'w') as f:
        json.dump({}, f)


if __name__ == '__main__':
    GRQ_ES_URL = sys.argv[1] + '/es/_search'
    S3_URL = sys.argv[2]  # ex. s3://aria-ops-dataset-bucket/datasets/slc

    s3_regex = re.search('s3:\/\/(.+)\/(.+)\/(.+)', S3_URL)  # use regex to parse the bucket and datatype
    bucket = s3_regex.group(1)
    data_type = s3_regex.group(3)

    PROD_MODE = False
    MODE = 'PERMANENT' if PROD_MODE else 'PRETEND'
    if PROD_MODE:
        print('PROD PURGE TURNED ON, WILL PURGE S3 FILES')
    else:
        print('PRETEND MODE ON, WILL NOT PURGE')

    STARTING_PREFIX = 'datasets/' + data_type + '/'  # Make sure you provide / in the end
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    bucket_resource = s3_resource.Bucket(bucket)
    END_LEVEL = 6  # this is how deep in s3 we track the data_set_id as directory
    PRODUCT_ID = 'orphaned_datasets_report'

    if not os.path.exists('%s/%s' % (os.getcwd(), PRODUCT_ID)):
        os.mkdir('%s/%s' % (os.getcwd(), PRODUCT_ID))

    CSV_FILE_NAME = '%s/%s/%s-%s-%s-%s.csv' % (os.getcwd(), PRODUCT_ID, PRODUCT_ID, data_type, datetime.now(), MODE)
    results_file = open(CSV_FILE_NAME, 'w')
    csv_writer = csv.writer(results_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(['bucket', 'key', 'data_set_id', 'purge_reason'])

    LOG_FILE_NAME = os.getcwd() + '/logs/' + 'orphaned_%s_%s_%s.log' % (data_type, datetime.now(), MODE)
    logger.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                       filename=LOG_FILE_NAME,
                       filemode='w',
                       level=logger.INFO)

    logger.info('PULLING ALL %s FROM GRQ' % data_type)
    all_data_sets_in_grq = pull_all_data_sets(GRQ_ES_URL, data_type)
    logger.info('PULLED %i %s FROM GRQ' % (len(all_data_sets_in_grq), data_type))

    counter = Counter()  # keep count of how many data sets we've processed
    traverse_s3_bucket(STARTING_PREFIX, 1)
    publish_dataset(PRODUCT_ID)
