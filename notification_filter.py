#!/usr/bin/python
import boto3
from botocore.client import Config
import sys
from json import loads
from kafka import KafkaConsumer

if len(sys.argv) != 6:
    print('Usage: ' + sys.argv[0] + ' <bucket> <filename> <topic name> <region name> <kafka endpoint>')
    sys.exit(1)

# bucket name as first argument
bucketname = sys.argv[1]
# Name of file to be uploaded
filename = sys.argv[2]
# Name of the topic we create
topic_name = sys.argv[3]
# Name of region to which we want to upload the file
region_name = sys.argv[4]
# The Kafka endpoint from which we want to receive updates
push_endpoint = sys.argv[4]

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

client = boto3.client('s3',
                      endpoint_url=endpoint,
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)

# this is standard AWS services call, using custom attributes to add Kafka endpoint information to the topic
ans = client.create_topic(Name="Kafka_Broker", Attributes={"push-endpoint": push_endpoint})


notification_conf = [{'Id': 'shtut',
                      'TopicArn': ans,
                      'Events': ['s3:ObjectSynced:*']
                      }]

print(client.put_bucket_notification_configuration(Bucket=bucketname,
                                                   NotificationConfiguration={
                                                       'TopicConfigurations': notification_conf}))

# Put objects to the relevant bucket
ans = client.upload_file(Filename=filename, Bucket=bucketname,
                         Key=filename)

if ans:
    # Create new Kafka consumer to listen to the message from Ceph
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=push_endpoint,
        value_deserializer=lambda x: loads(x))

    consumer_list = [message.value for message in consumer]

    print(consumer_list)

    for message in consumer_list:
        if message['s3']['bucket']['name'] == bucketname and message['object']['key'] == filename \
                and message['eventName'] == "ceph:ObjectSynced":
            site = message['x-amz-id-2']
            print(f'Object {filename} put in {bucketname} successfully to site {site}')
