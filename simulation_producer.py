#!/usr/bin/python
import boto3
import sys
from json import dumps
from kafka import KafkaProducer
import topic_test

# endpoint and keys from vstart
endpoint = 'http://127.0.0.1:8000'
access_key = '0555b35654ad1656d804'
secret_key = 'h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='

producer = KafkaProducer(bootstrap_servers=endpoint,
                         value_serializer=lambda x:
                         dumps(x))

info = topic_test.main()

data = {
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion":"zonegroup",
        "x-amz-id-2":"rgw-zone-zonegroup",
        "eventTime":"sync-time",
        "eventName":"ceph:ObjectSynced",
        "s3":{
            "s3SchemaVersion":"1.0",
            "configurationId":"notification-id",
            "bucket":{
                "name":"",
                "ownerIdentity":{
                    "principalId":""
                },
                "arn":"",
                "id":""
            },
            "object":{
                "key":"123",
                "eTag":"132",
                "versionId":"144",
                "sequencer":"156",
            }
        },
        "eventId":"123",
        "opaqueData":"564"
    }

producer.send(info[0], data)