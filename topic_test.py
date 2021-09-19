#!/usr/bin/python

import boto3
import sys
from botocore.client import Config


def main():
    if len(sys.argv) == 4:
        # topic name as first argument
        topic_name = sys.argv[1]
        # region name as second argument
        region_name = sys.argv[2]
        # push_endpoint as third argument
        push_endpoint = sys.argv[3]
    else:
        print('Usage: ' + sys.argv[0] + ' <topic name> <region name> <kafka endpoint>')
        sys.exit(1)

    # endpoint and keys from vstart
    endpoint = 'http://127.0.0.1:8000'
    access_key = '0555b35654ad1656d804'
    secret_key = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

    client = boto3.client('sns',
                          endpoint_url=endpoint,
                          aws_access_key_id=access_key,
                          region_name=region_name,
                          aws_secret_access_key=secret_key,
                          config=Config(signature_version='s3'))

    # to see the list of available "regions" use:
    # radosgw-admin realm zonegroup list

    # this is standard AWS services call, using custom attributes to add Kafka endpoint information to the topic
    ans = client.create_topic(Name="Kafka_Broker", Attributes={"push-endpoint": push_endpoint})

    return ans, topic_name, push_endpoint


main()