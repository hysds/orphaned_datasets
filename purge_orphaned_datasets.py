import os
import sys
import boto3
import urllib3
from datetime import datetime
import logging as logger

from hysds.celery import app
from util import OrphanedDatasetsFinder, read_context, pull_all_data_sets, publish_dataset

urllib3.disable_warnings()


if __name__ == '__main__':
    # python purge_orphaned_datasets.py aria-ops-dataset-bucket slc PRETEND
    GRQ_ES_URL = app.conf['GRQ_ES_URL'] + '/_search'

    try:
        bucket = sys.argv[1]
        dataset_type = sys.argv[2]
        mode = sys.argv[3]  # ['PRETEND', 'PERMANENT']
    except Exception as e:
        cxt = read_context()
        bucket = cxt['bucket']
        dataset_type = cxt['dataset_type']
        mode = cxt['mode']

    if mode == 'PERMANENT':
        print('PROD PURGE TURNED ON, WILL PURGE S3 FILES')
    else:
        print('PRETEND mode ON, WILL NOT PURGE')

    current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    starting_prefix = 'datasets/' + dataset_type + '/'  # Make sure you provide / in the end
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    bucket_resource = s3_resource.Bucket(bucket)
    product_id = 'orphaned_datasets_report-%s-%s' % (current_timestamp, dataset_type)

    if not os.path.exists('%s/%s' % (os.getcwd(), product_id)):
        os.mkdir('%s/%s' % (os.getcwd(), product_id))

    csv_filename = '%s/%s/%s-%s-%s-%s.csv' % (os.getcwd(), product_id, product_id, dataset_type, current_timestamp, mode)
    results_file = open(csv_filename, 'w')

    LOG_FILE_NAME = os.getcwd() + '/orphaned_%s_%s_%s.log' % (dataset_type, current_timestamp, mode)
    logger.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                       filename=LOG_FILE_NAME,
                       filemode='w',
                       level=logger.INFO)

    logger.info('PULLING ALL %s FROM GRQ' % dataset_type)
    all_data_sets_in_grq = pull_all_data_sets(GRQ_ES_URL, dataset_type)
    logger.info('PULLED %i %s FROM GRQ' % (len(all_data_sets_in_grq), dataset_type))

    dataset_finder = OrphanedDatasetsFinder(mode, s3_client, bucket_resource, bucket, all_data_sets_in_grq,
                                            results_file, logger)
    dataset_finder.traverse_s3_bucket(starting_prefix, 1)
    publish_dataset(product_id)
