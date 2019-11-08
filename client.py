import json

import requests
from google.protobuf.json_format import MessageToJson

from static import dataCommunication_pb2

request = dataCommunication_pb2.Request()
request.branch = 'Dell'
request.RFW_ID = "jf3458rw-3rjdc134fr-a1eif03r52"
request.workload_metric = "CPU"
request.batch_unit = 5
request.batch_id = 1
request.batch_size = 2
request.dataset_type = 'training'
payload = request.SerializeToString()


headers = {
        'Content-Type': 'application/x-protobuf'
    }

result = requests.post("http://127.0.0.1:5000/v1/batches/proto", data = payload, headers=headers)
res = dataCommunication_pb2.Response()
request2 = dataCommunication_pb2.Request()
res.ParseFromString(result.content)
j = MessageToJson(res)
sValue = json.loads(j)
for k in sValue.keys():
  if str(type(sValue[k]))!="<class 'dict'>":
    print(k+':'+ str(sValue[k]))
