#!/usr/bin/python
import boto3
import sys
from urllib.parse import urlparse
from botocore.client import Config
from json import loads
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic


def put_object(filename):
    client = boto3.client('s3')
    #  Save to S3
    ans = client.upload_file(Filename=filename, Bucket=bucketname,
                             Key=filename)
    return ans


if len(sys.argv) != 5:
    print('Usage: ' + sys.argv[0] + ' [region name] <bucket> <topic name> <notification Id>')
    sys.exit(1)

region_name = sys.argv[1]
bucketname = sys.argv[2]
topic_name = sys.argv[3]
notification_id = sys.argv[4]

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key = '0555b35654ad1656d804'
secret_key = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

# TODO: Replace with Kafka connection

admin_client = KafkaAdminClient(
    bootstrap_servers=endpoint,
    client_id=access_key
)

admin_client.create_topics(new_topics=[topic_name], validate_only=False)

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=endpoint,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=notification_id,
    value_deserializer=lambda x: loads(x.decode('utf-8')))

print(consumer)

topic_arn = new_topic['CreateTopicResponse']['CreateTopicResult']['TopicArn']

# regex filter on the object name and metadata based filtering are extension to AWS S3 API
# bucket and topic should be created beforehand

topic_conf_list = [{'Id': notification_id,
                    'TopicArn': topic_arn,
                    'Events': ['s3:ObjectSynced:*'],
                    'Filter': {
                        # 'Metadata': {
                        #     'FilterRules': [{'Name': 'x-amz-meta-foo', 'Value': 'bar'},
                        #                     {'Name': 'x-amz-meta-hello', 'Value': 'world'}]
                        # },
                        # 'Key': {
                        #     'FilterRules': [{'Name': 'regex', 'Value': '([a-z]+)'}]
                        # }
                    }}]

print(client.put_bucket_notification_configuration(Bucket=bucketname,
                                                   NotificationConfiguration={'TopicConfigurations': topic_conf_list}))

# TODO: Put object
filename = "blabla.jpeg"
obj_put = put_object(filename)

# TODO: Kafka notification through consumer
if obj_put:
    print(f'Object {filename} put in {bucketname} successfully')
