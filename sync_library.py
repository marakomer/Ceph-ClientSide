#!/usr/bin/python
import boto3
from botocore.client import Config
from json import loads
from kafka import KafkaConsumer
import threading
import base64

class SyncLibrary:
    def __init__(self, *, rgw_endpoint, kafka_endpoint, aws_access_key_id, aws_secret_access_key, region_name):
        self._s3_client = boto3.client('s3',
                          endpoint_url=rgw_endpoint,
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)        
        self._sns_client = boto3.client('sns',
                          region_name=region_name,
                          endpoint_url=rgw_endpoint,
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key,
                          config=Config(signature_version='s3'))
        self._kafka_endpoint = kafka_endpoint
        
        self._objects = {}
        
    def _spin(consumer, bucketname, key, timeout, max_zones):
        for msg in consumer:
            message = msg.value
            print(message)
            if message['s3']['bucket']['name'] == bucketname and message["s3"]['object']['key'] == key \
                    and message['eventName'] == "ceph:ObjectSynced":
                print("Object "+ filename+" put in "+bucketname+" successfully to site "+site)
                self._objects[key][0]+= 1
                if self._objects[key] >= max_zones:
                    self._objects[key][1] = "success"
                    return
    
    def get_replication_status(object_key):
        if not self._objects[object_key]:
            return None
        return self._objects[object_key][1]
    
    def upload_file(bucket_name, file_name, object_key=None, timeout=-1, zones=0):
        if not object_key:
            object_key = file_name
    
        topic_name = base64.b16encode((bucket_name + file_name + self._kafka_endpoint).encode()).decode("utf-8")
        arn = self._sns_client.create_topic(Name=topic_name,
                              Attributes={"push-endpoint": self._kafka_endpoint})["TopicArn"]
                              
        notification_conf = [{'Id': 'sync-library',
                              'TopicArn': arn,
                              'Events': ['s3:ObjectSynced:*']
                              }]

        self._s3_client.put_bucket_notification_configuration(Bucket=bucket_name,
                                                        NotificationConfiguration={
                                                            'TopicConfigurations': notification_conf})

        # Create new Kafka consumer to listen to the message from Ceph
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=self._kafka_endpoint,
            value_deserializer=lambda x: loads(x.decode("utf-8")))
            
        self._objects[object_key] = (0, "pending")
        
        thread = threading.Thread(target=lambda x: self._spin(x), args=(consumer, bucket_name, object_key, timeout, zones))
        thread.start()
            
        # Put objects to the relevant bucket
        ans = s3_client.upload_file(Filename=file_name, Bucket=bucket_name,
                                    Key=object_key)
        