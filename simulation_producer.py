#!/usr/bin/python
from json import dumps
from kafka import KafkaProducer

# endpoint and keys from vstart
endpoint = '127.0.0.1:8000'
access_key = '0555b35654ad1656d804'
secret_key = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

push_endpoint = '127.0.0.1:9092'
producer = KafkaProducer(bootstrap_servers=push_endpoint,
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

data = {
    "eventVersion": "2.1",
    "eventSource": "aws:s3",
    "awsRegion": "zonegroup",
    "x-amz-id-2": "rgw-zone-zonegroup",
    "eventTime": "sync-time",
    "eventName": "ceph:ObjectSynced",
    "s3": {
        "s3SchemaVersion": "1.0",
        "configurationId": "notification-id",
        "bucket": {
            "name": "mybucket",
            "ownerIdentity": {
                "principalId": ""
            },
            "arn": "",
            "id": ""
        },
        "object": {
            "key": "trial.jpg",
            "eTag": "132",
            "versionId": "144",
            "sequencer": "156",
        }
    },
    "eventId": "123",
    "opaqueData": "564"
}

producer.send("6D796275636B6574747269616C2E6A70673132372E302E302E313A39303932", data)
producer.flush()