import boto3
import json
from datetime import datetime
import time

my_stream_name = 'TestStream'

session = boto3.Session(
    profile_name='asurion-poc.amadevops',
    region_name='ap-northeast-1'
)

kinesis_client = session.client("kinesis")


response = kinesis_client.describe_stream(StreamName=my_stream_name)

my_shard_id = response['StreamDescription']['Shards'][0]['ShardId']

shard_iterator = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                      ShardId=my_shard_id,
                                                      ShardIteratorType='LATEST')

my_shard_iterator = shard_iterator['ShardIterator']

record_response = kinesis_client.get_records(ShardIterator=my_shard_iterator,
                                              Limit=2)

while 'NextShardIterator' in record_response:
    record_response = kinesis_client.get_records(ShardIterator=record_response['NextShardIterator'],
                                                  Limit=2)

    # for rec in record_response['Records']:
        
    my_json = record_response['Records'][0]['Data'].decode('utf8').replace("'", '"')

        # Load the JSON to a Python list & dump it back out as formatted JSON
    data = json.loads(my_json)
    s = json.dumps(data, indent=4, sort_keys=True)
    print(s)

    time.sleep(1)

    # wait for 5 seconds
   
