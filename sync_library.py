#!/usr/bin/python
import boto3
from botocore.client import Config
from json import loads
from kafka import KafkaConsumer
import threading
import time
import base64

class SyncLibrary:
    def __init__(self, *, rgw_endpoints, kafka_endpoint, access_key, secret_key, region_name):
        self._s3_clients = {}
        self._sns_clients = {}
        for zone in rgw_endpoints:
            self._s3_clients[zone] = boto3.client('s3',
                              endpoint_url=rgw_endpoints[zone],
                              aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key)        
            self._sns_clients[zone] = boto3.client('sns',
                              region_name=region_name,
                              endpoint_url=rgw_endpoints[zone],
                              aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key,
                              config=Config(signature_version='s3'))
        self._kafka_endpoint = kafka_endpoint
        
        self._objects = {}
        
    def _spin(self, consumer, bucketname, key, timeout, max_zones):
        for msg in consumer:
            message = msg.value
            if message['s3']['bucket']['name'] == bucketname and message["s3"]['object']['key'] == key \
                    and message['eventName'] == "ceph:ObjectSynced":
                self._objects[key][0]+= 1
                if self._objects[key][0] >= max_zones:
                    self._update_object_status(key, "success")
                    break
        consumer.close()
    
    
    def _timeout(self, consumer, key, thread, timeout):
        thread.join(timeout)
        consumer.close()
        if self.get_replication_status(key) == "pending":
            self._update_object_status(key, "failed")
    
    def _update_object_status(self, key, status):
        if key not in self._objects:
            self._objects[key] = [0, status, None]
        else:
            self._objects[key][0] = 0
            self._objects[key][1] = status
        if self._objects[key][2]:
            self._objects[key][2](status)
    
    def set_replication_callback(self, object_key, callback):
        if object_key not in self._objects:
            self._objects[object_key] = [0, None, callback]
        else:
            self._objects[object_key][2] = callback
    
    def get_replication_status(self, object_key):
        if object_key not in self._objects:
            return None
        return self._objects[object_key][1]
    
    def upload_file(self, zone_name, bucket_name, file_name, object_key=None, *, timeout=-1, zones=0):
        if not object_key:
            object_key = file_name
    
        topic_name = base64.b16encode((bucket_name + file_name + self._kafka_endpoint).encode()).decode("utf-8")
        for zone in self._s3_clients:
            arn = self._sns_clients[zone].create_topic(Name=topic_name,
                                  Attributes={"push-endpoint": "http://" + self._kafka_endpoint})["TopicArn"]
                                  
            notification_conf = [{'Id': 'sync-library',
                                  'TopicArn': arn,
                                  'Events': ['s3:ObjectSynced:*']
                                  }]

            self._s3_clients[zone].put_bucket_notification_configuration(Bucket=bucket_name,
                                                            NotificationConfiguration={
                                                                'TopicConfigurations': notification_conf})

        # Create new Kafka consumer to listen to the message from Ceph
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=self._kafka_endpoint,
            value_deserializer=lambda x: loads(x.decode("utf-8")))
                                                
        self._update_object_status(object_key, "pending")
        
        thread = threading.Thread(target=lambda: self._spin(consumer, bucket_name, object_key, timeout, zones), daemon=True)
        thread.start()
        
        if timeout >= 0:
            timeout_thread = threading.Thread(target=lambda: self._timeout(consumer, object_key, thread, timeout), daemon=True)
            timeout_thread.start()
            
        # Put objects to the relevant bucket
        ans = self._s3_clients[zone_name].upload_file(Filename=file_name, Bucket=bucket_name,
                                    Key=object_key)
