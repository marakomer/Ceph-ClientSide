#!/usr/bin/python
import boto3
import sys
from json import loads
from kafka import KafkaConsumer
import topic_test


if len(sys.argv) != 4:
    print('Usage: ' + sys.argv[0] + ' <bucket> <filename>')
    sys.exit(1)

# bucket name as first argument
bucketname = sys.argv[1]
# Name of file to be uploaded
filename = sys.argv[2]

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key = '0555b35654ad1656d804'
secret_key = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

client = boto3.client('s3',
                      endpoint_url=endpoint,
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)

info = topic_test.main()

notification_conf = [{'Id': 'shtut',
                      'TopicArn': info[0],
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
        info[1],
        bootstrap_servers=endpoint,
        value_deserializer=lambda x: loads(x))

    consumer_list = [message.value for message in consumer]

    print(consumer_list)

    for message in consumer_list:
        if message['s3']['bucket']['name'] == bucketname and message['object']['key'] == filename:
            print(f'Object {filename} put in {bucketname} successfully')