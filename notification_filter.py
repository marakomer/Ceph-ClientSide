#!/usr/bin/python
import boto3
import sys
from urllib.parse import urlparse
from botocore.client import Config

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

client = boto3.client('sns',
                      endpoint_url=endpoint,
                      aws_access_key_id=access_key,
                      region_name=region_name,
                      aws_secret_access_key=secret_key,
                      config=Config(signature_version='s3'))

# to see the list of available "regions" use:
# radosgw-admin realm zonegroup list


# this is standard AWS services call, using custom attributes to add AMQP endpoint information to the topic
# TODO: Replace with Kafka connection
endpoint_args = 'push-endpoint=amqp://127.0.0.1:5672&amqp-exchange=ex1&amqp-ack-level=broker'
attributes = {nvp[0]: nvp[1] for nvp in urlparse.parse_qsl(endpoint_args, keep_blank_values=True)}

new_topic = client.create_topic(Name=topic_name, Attributes=attributes)

print(new_topic)

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

# TODO: Kafka notification through consumer
