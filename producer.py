import json
import boto3

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
  DeleteRowsEvent,
  UpdateRowsEvent,
  WriteRowsEvent,
)

def main():

  session = boto3.Session(
    profile_name='asurion-poc.amadevops',
    region_name='ap-northeast-1'
  )

  kinesis = session.client("kinesis")

  stream = BinLogStreamReader(
    connection_settings= {
      "host": "yourhost",
      "port": 3306,
      "user": "dbadmin",
      "passwd": ""},
    server_id=100,
    blocking=True,
    resume_stream=True,
    only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])

  for binlogevent in stream:
    for row in binlogevent.rows:
      event = {"schema": binlogevent.schema,
      "table": binlogevent.table,
      "type": type(binlogevent).__name__,
      "row": row
      }

      kinesis.put_record(StreamName="TestStream", Data=json.dumps(event), PartitionKey="default")
      print(json.dumps(event))

if __name__ == "__main__":
   main()
