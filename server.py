from flask import Flask, request, json, make_response
import pandas as pd
from flask_restful import reqparse
from google.protobuf import json_format

from static import dataCommunication_pb2

app = Flask(__name__)
RFW = "jf3458rw-3rjdc134fr-a1eif03r52"
chunk_list = []


def getCol(value):
    v = value.lower()
    functions = {
        'cpu': 1,
        'networkin': 2,
        'networkout': 3,
        'memory': 4
    }
    return functions.get(v)


@app.route('/v1/batches/json', methods=['POST'])
# use json
def receive_json_request():
    print("json request received")
    parser = reqparse.RequestParser()
    parser.add_argument('branch', type=str)
    parser.add_argument('datasetType', type=str)
    parser.add_argument('workloadMetric', type=str)
    parser.add_argument('batchUnit', type=int)
    parser.add_argument('batchId', type=int)
    parser.add_argument('batchSize', type=int)
    parser.add_argument('RFWID', type=str)
    args = parser.parse_args()
    result = request_analy_json(args)
    return make_response(result, 200)




@app.route('/v1/batches/proto', methods=['POST','GET'])
# use proto
def receive_proto_request():
    print("receive proto request")
    reqs = dataCommunication_pb2.Request()
    reqs.ParseFromString(request.get_data().replace(b'\r\n', b'\n'))
    if rfw_check(reqs.RFW_ID) is False:
        return
    result = request_analy_proto(reqs)
    print("final result is :",result)
    parsed_pb = json_format.Parse(result, dataCommunication_pb2.Response(), ignore_unknown_fields=False)
    return make_response(parsed_pb.SerializeToString(), 200)


def rfw_check(str):
    if str != RFW:
        return False
    return True


def request_analy_proto(reqs):
       return _getDataFrame(reqs)


def _getDataFrame(reqs):
    file_name = "./static/"
    type = reqs.branch
    if type.lower() == 'dell':
        file_name = '{}{}'.format(file_name,'NDBench-')
    else :
        file_name = '{}{}'.format(file_name,'DVD-')
    file_name = '{}{}{}'.format(file_name,reqs.dataset_type,'.csv')
    print('file name is: ',file_name)
    metric = reqs.workload_metric
    col_index = getCol(metric)
    batchSize = reqs.batch_size
    batchID = reqs. batch_id
    unit = reqs.batch_unit
    reader = pd.read_csv(file_name, usecols=[col_index], chunksize=unit, iterator=True)
    chunk_str = ''
    for chunk in reader:
        chunk_list.append(chunk)
    count = 0;
    while count < batchSize:
        count += 1
        next_str = '[{}]'.format(chunk_list[batchID + count].to_json(orient="values", force_ascii=False).replace('[', '').\
            replace(']',''))
        print("Slice: ", next_str)
        chunk_str = '{},{}'.format(chunk_str,next_str)
        new_str = chunk_str[1:]
        print("After Refactor: ", new_str)
        chunk_json = eval(new_str)
    return to_json(batchID, metric, chunk_json)

def request_analy_json(args):
    if args.get('RFWID') != RFW:
        return
    file_name = "./static/"
    type = args.get('branch')
    metric = args.get('workloadMetric')
    col_index = getCol(metric)
    batchUnit = args.get('batchUnit')
    batchSize = args.get('batchSize')
    dataset_type = args.get('datasetType')
    batchID = args.get('batchId')
    if type.lower() == 'dell':
        print("request is dell dataset")
        file_name = '{}{}'.format(file_name,'NDBench-')
    else :
        print("request is netflix dataset")
        file_name = '{}{}'.format(file_name,'DVD-')
    file_name = '{}{}{}'.format(file_name,dataset_type,'.csv')
    print('file name is: ',file_name)
    reader = pd.read_csv(file_name, usecols=[col_index], chunksize=batchUnit,
                                 iterator=True)
    chunk_str = ''
    for chunk in reader:
        chunk_list.append(chunk)
    count = 0
    while count < batchSize:
        count += 1
        next_str = '[{}]'.format(chunk_list[batchID + count].to_json(orient="values", force_ascii=False).replace('[', '').\
            replace(']',''))
        print("Slice: ", next_str)
        chunk_str = '{},{}'.format(chunk_str,next_str)
        new_str = chunk_str[1:]
        print("After Refactor: ", new_str)
        chunk_json = eval(new_str)
    return to_json(batchID, metric, chunk_json)


# {
#     "SUCCESS": "SUCCESS",
#     "workload_data": {
#         "0": [
#             [
#                 260661198
#             ],
#             [
#                 280470095
#             ],
#             [
#                 286492234
#             ],
#             [
#                 280274041
#             ],
#             [
#                 267113409
#             ]
#         ],
#         "1": [
#             [
#                 260661198
#             ],
#             [
#                 280470095
#             ],
#             [
#                 286492234
#             ],
#             [
#                 280274041
#             ],
#             [
#                 267113409
#             ]
#         ]
#     }
# }

def to_json(batch_id, metrics, chunk_str):
    before_json = {
        'last_batch_ID': batch_id, 'RFW_ID': RFW, 'workload_metrics': metrics, 'workload_data': chunk_str
    }
    print('After Jsonify: ', json.dumps(before_json))
    # rr = {"RFW_ID": "jf3458rw-3rjdc134fr-a1eif03r52", "last_batch_ID": 1, "workload_data":
    #     ["239635160", "215437697"], "workload_metrics": "cpu"}
    return json.dumps(before_json)


def after_request(resp):
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp


app.after_request(after_request)

if __name__ == '__main__':
    app.run(port=5000, debug=True)
