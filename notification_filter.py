#!/usr/bin/python
import boto3
from botocore.client import Config
import sys
from json import loads
from kafka import KafkaConsumer
from botocore.client import ClientError

if len(sys.argv) != 4:
    print('Usage: ' + sys.argv[0] + ' <bucket> <filename> <kafka endpoint>')
    sys.exit(1)

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key = '0555b35654ad1656d804'
secret_key = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

# bucket name as first argument
bucketname = sys.argv[1]

s3_client = boto3.client('s3',
                         endpoint_url=endpoint,
                         aws_access_key_id=access_key,
                         aws_secret_access_key=secret_key)

# try:
#     s3_client.head_bucket(Bucket=bucketname)
#
# except ClientError:
#     # The bucket does not exist or you have no access.
#     print("This bucket does not exist or you are missing permissions!")

# Name of file to be uploaded
filename = sys.argv[2]
# The Kafka endpoint from which we want to receive updates
push_endpoint = "http://" + sys.argv[3]

sns_client = boto3.client('sns',
                          region_name="us-east-1",
                          endpoint_url=endpoint,
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key,
                          config=Config(signature_version='s3'))

# this is standard AWS services call, using custom attributes to add Kafka endpoint information to the topic
arn = sns_client.create_topic(Name=str(hash(bucketname + filename + push_endpoint)),
                              Attributes={"push-endpoint": push_endpoint})["TopicArn"]
                              
notification_conf = [{'Id': 'shtut',
                      'TopicArn': arn,
                      'Events': ['s3:ObjectSynced:*']
                      }]

s3_client.put_bucket_notification_configuration(Bucket=bucketname,
                                                NotificationConfiguration={
                                                    'TopicConfigurations': notification_conf})

# Create new Kafka consumer to listen to the message from Ceph
consumer = KafkaConsumer(
    arn,
    bootstrap_servers=sys.argv[2],
    value_deserializer=lambda x: loads(x))

# Put objects to the relevant bucket
ans = s3_client.upload_file(Filename=filename, Bucket=bucketname,
                            Key=filename)

consumer_list = [message.value for message in consumer]

for message in consumer_list:
    if message['s3']['bucket']['name'] == bucketname and message['object']['key'] == filename \
            and message['eventName'] == "ceph:ObjectSynced":
        site = message['x-amz-id-2']
        print("Object "+ filename+" put in "+bucketname+" successfully to site "+site)
