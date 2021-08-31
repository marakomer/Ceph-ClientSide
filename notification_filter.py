#!/usr/bin/python
import boto3
import sys
from urllib.parse import urlparse
from botocore.client import Config
from json import loads
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import topic_test


def put_object(message):
    region = message['awsRegion']
    account_id = message['id']
    resource = '*'  # Not sure about this
    bucket_name = message['s3']['bucket']['name']
    filename = ""  # Where is it?
    client = boto3.client(f'arn:aws:s3:{region}:{account_id}:accesspoint/{resource}')
    #  Save to S3
    ans = client.upload_file(Filename=filename, Bucket=bucket_name,
                             Key=filename)
    if ans:
        print(f'Object {filename} put in {bucket_name} successfully')


topic_conf_list = [{'Id': 'shtut',
                    'TopicArn': topic_test.main(),
                    'Events': ['s3:ObjectSynced:*'],
                    }]

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key = '0555b35654ad1656d804'
secret_key = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='


# Create new Kafka consumer to listen to the message from Ceph
consumer = KafkaConsumer(
    "topic_name",
    bootstrap_servers=endpoint,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=0xF000,
    value_deserializer=lambda x: loads(x.decode('utf-8')))

print(consumer)

consumer_list = [message.value for message in consumer]

# Put objects to the relevant bucket
for message in consumer_list:
    put_object(message)



# regex filter on the object name and metadata based filtering are extension to AWS S3 API
# bucket and topic should be created beforehand




