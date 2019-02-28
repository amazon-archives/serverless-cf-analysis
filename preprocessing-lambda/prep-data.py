from __future__ import print_function

import os
import json
import urllib
import boto3
import time
import sys
import random
from gzip import GzipFile
from io import BytesIO
from user_agents import parse

print('Loading function')

s3 = boto3.client('s3')
firehose = boto3.client('firehose')
firehose_stream = os.environ['KINESIS_FIREHOSE_STREAM']

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    start = time.clock()
    print("Processing log file")

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    dist_name = key.split("/")[-1].split(".")[0]

    print("Log bucket {}".format(bucket))
    print("Firehose stream {}".format(firehose_stream))

    try:
        print("GET key {}".format(key))
        response = s3.get_object(Bucket=bucket, Key=key)
        bytestream = BytesIO(response['Body'].read())
        data = GzipFile(None, 'rb', fileobj=bytestream).read().decode('utf-8')

        # batch == array of records
        records = []
        
        for line in data.strip().split("\n"):
            if not line.startswith("#"):
                try:
                    line = line.strip().encode("utf-8", "ignore")
                    ua = parse(line.split("\t")[10])

                    # record == dict with key 'Data'
                    d = {
                        'Data': line + '\t' + str(ua.browser.family) + '\t' + str(ua.os.family) + '\t' + str(ua.is_bot) + '\t' + key + '\t' + dist_name + '\n'                    
                    }

                    # add dict to records
                    records.insert(len(records), d)

                    # max batch length 500
                    if len(records) > 499:
                        try:
                            print("PUT {} records".format(len(records)))
                            r = put_firehose(firehose_stream, records)

                        except Exception as e:
                            print("Exception calling kinesis firehose ".format(e))
                        
                        # initialize records
                        records = []
                except Exception as e:
                    print(e)
                    print("Exception during utf8 conversion")

        # put the last batch of <500
        if len(records) > 0:
            try:
                print("PUT last {} records".format(len(records)))
                r = put_firehose(firehose_stream, records)

            except Exception as e:
                print("Exception calling kinesis firehose ".format(e))
                
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Check key, bucket and region.'.format(key, bucket))
        raise e

    print("Finished processing file")

def put_firehose(firehose_stream, records):
    r = firehose.put_record_batch(
        DeliveryStreamName=firehose_stream,
        Records=records
    )

    backoff_time = 0.5
    failed_count = r['FailedPutCount']
    while failed_count > 0:
        print("FailedPutCount {}, retrying failed records (request id: {})".format(failed_count,r['ResponseMetadata']['RequestId']))
        failed_records = [records[i] for i, record in enumerate(r['RequestResponses']) if 'ErrorCode' in record]
        backoff_time = backoff_time * 2
        print("Sleep {}".format(backoff_time))
        time.sleep(backoff_time)
        r = firehose.put_record_batch(
            DeliveryStreamName=firehose_stream,
            Records=failed_records
        )
        failed_count = r['FailedPutCount']