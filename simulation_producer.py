#!/usr/bin/python
from json import dumps
from kafka import KafkaProducer

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key = '0555b35654ad1656d804'
secret_key = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

producer = KafkaProducer(bootstrap_servers=endpoint,
                         value_serializer=lambda x:
                         dumps(x))

push_endpoint = ''
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
            "name": "456",
            "ownerIdentity": {
                "principalId": ""
            },
            "arn": "",
            "id": ""
        },
        "object": {
            "key": "123.jpg",
            "eTag": "132",
            "versionId": "144",
            "sequencer": "156",
        }
    },
    "eventId": "123",
    "opaqueData": "564"
}

producer.send(push_endpoint, data)
