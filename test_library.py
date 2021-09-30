#!/usr/bin/python
import sync_library

library = sync_library.SyncLibrary(rgw_endpoint="http://127.0.0.1:8000", kafka_endpoint="127.0.0.1:9092", access_key="0555b35654ad1656d804", secret_key="h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==", region_name="us-east-1")

library.set_replication_callback("trial.jpg", lambda status: print("hello: " + status))

library.upload_file("mybucket", "trial.jpg", zones=1, timeout=2)

while True:
    pass