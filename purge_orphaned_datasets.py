import os
import sys
import json
import boto3
import requests, urllib3
from datetime import datetime
import logging as logger
import csv
import argparse
import pytz
from util import Counter, pull_all_data_sets, check_file_date_s3, delete_s3_folder

urllib3.disable_warnings()

# TODO: ADD IMPORT CELERY CONFIGS FOR THE ELASTICSEARCH URL AND S3 KEYS


def load_context():
    # loads the context file into a dict
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except:
        raise Exception('unable to parse _context.json from work directory')


def traverse_s3_bucket(prefix, level):
    '''
    :param prefix: str (ex. aria-ops-dataset-bucket/datasets/interferogram/v2.0.1/2015/06/05/S1-GUNW.../
    :param level: int: how deep we crawl into our s3 bucket
    :variables:
        BUCKET = bucket name
        END_LEVEL = furthest level we will travel down a s3 bucket
        s3_client = s3 client API, used to get the common prefix
        s3_resource = used to purge the s3 bucket
        logger = log handler
        csv_writer = csv handler
    :return: void
    '''

    result = s3_client.list_objects(Bucket=BUCKET, Prefix=prefix, Delimiter='/')
    client_results = result.get("CommonPrefixes")

    if not client_results and level < END_LEVEL:  # edge case so nothing is accidentally deleted
        logger.info("Invalid S3 path: does not match convention dataset/version/YYYY/MM/DD/dataset/ %s" % prefix)

    if not client_results or level == END_LEVEL:  # base case
        counter.increment()
        if counter.get_count() % 50 == 0:
            print(counter.get_count())

        split_prefix = prefix.split('/')  # splitting prefix paths
        data_set_id = split_prefix[-2]
        version = split_prefix[2]  # also want to keep the dataset version consistent
        data_set = (data_set_id, version)  # to compare against the GRQ results (dataset_id, version)

        if data_set in all_data_sets_in_grq:
            logger.info("%i %s %s found in GRQ" % (counter.get_count(), data_set_id, version))
        else:
            logger.error("%i %s %s NOT FOUND IN GRQ" % (counter.get_count(), data_set_id, version))
            data_set_json = prefix + data_set_id + ".dataset.json"  # valid data sets have this file in s3
            data_json_last_modified = check_file_date_s3(s3_client, BUCKET, data_set_json)

            if not data_json_last_modified:  # .dataset.json not found in s3, therefore it is not a valid dataset
                logger.error("%s NOT FOUND IN S3" % data_set_json)
                csv_writer.writerow([BUCKET, prefix, data_set_id, "dataset.json NOT FOUND IN S3"])

                if PROD_MODE:  # purge data_set_id
                    logger.info("Sent to purge: %s" % prefix)
                    delete_s3_folder(bucket_resource, prefix)
                else:
                    logger.info("Not sent to purge. Add -f flag to purge dataset")

            else:
                logger.info("%s FOUND IN S3" % data_set_json)
                # check how old the .dataset.json file is, if its older than 24 hours, then purge
                now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)

                if (now_utc - data_json_last_modified).total_seconds() > 86400:  # if greater than 24 hours
                    logger.info("%s IS OLDER THAN 24 HOURS, WILL DELETE" % data_set_json)
                    csv_writer.writerow([BUCKET, prefix, data_set_id, "dataset.json FILE IS OLDER THAN 24 HOURS"])

                    if PROD_MODE:
                        logger.info("Sent to purge: %s" % prefix)
                        delete_s3_folder(bucket_resource, prefix)
                    else:
                        logger.info("Not sent to purge. Add -f flag to purge dataset")

                else:
                    logger.info("%s is NOT OLDER than 1 day, will not delete" % data_set_json)

        results_file.flush()  # in case anything breaks we still have a audit file of what was deleted
        return

    for o in client_results:  # recursive step
        next_prefix = o.get("Prefix")
        traverse_s3_bucket(next_prefix, level + 1)  # recurse on each "sub-directory" in s3


if __name__ == '__main__':
    cxt = load_context()

    GRQ_ES_URL = cxt.get("grq_es_url")
    if not GRQ_ES_URL:
        from hysds.celery import app

        GRQ_ES_URL = app.conf["GRQ_ES_PUB_IP"] + "/es/_search"

    PROD_MODE = cxt.get("prod_mode")
    MODE = "PERMANENT" if PROD_MODE else "PRETEND"
    if PROD_MODE:
        print("PROD PURGE TURNED ON, WILL PURGE S3 FILES")
    else:
        print("PRETEND MODE ON, WILL NOT PURGE")

    BUCKET = "aria-ops-dataset-bucket"
    DATA_TYPE = cxt.get("data_type")

    if not BUCKET or not DATA_TYPE:
        logger.error("Missing input argument")
        sys.exit("Missing input argument")

    # HARD CODED VARIABLES
    ########################################################################################
    STARTING_PREFIX = "datasets/" + DATA_TYPE + "/"  # Make sure you provide / in the end
    s3_client = boto3.client("s3")
    s3_resource = boto3.resource("s3")
    bucket_resource = s3_resource.Bucket(BUCKET)
    END_LEVEL = 6  # this is how deep in s3 we track the data_set_id as directory

    if not os.path.exists(os.getcwd() + "/logs"):
        os.mkdir(os.getcwd() + "/logs")

    CSV_FILE_NAME = "logs/orphaned_%s_%s_%s.csv" % (DATA_TYPE, datetime.now(), MODE)
    results_file = open(CSV_FILE_NAME, 'w')
    csv_writer = csv.writer(results_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    csv_writer.writerow(["bucket", "key", "data_set_id", "purge_reason"])

    LOG_FILE_NAME = os.getcwd() + "/logs/" + "orphaned_%s_%s_%s.log" % (DATA_TYPE, datetime.now(), MODE)
    logger.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                       filename=LOG_FILE_NAME,
                       filemode='w',
                       level=logger.INFO)

    logger.info("PULLING ALL %s FROM GRQ" % DATA_TYPE)
    all_data_sets_in_grq = pull_all_data_sets(GRQ_ES_URL, DATA_TYPE)
    logger.info("PULLED %i %s FROM GRQ" % (len(all_data_sets_in_grq), DATA_TYPE))

    counter = Counter()  # keep count of how many data sets we've processed
    traverse_s3_bucket(STARTING_PREFIX, 1)
