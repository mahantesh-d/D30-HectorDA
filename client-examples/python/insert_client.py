import grpc
import d20_pb2
import sys
def execute(stub,req):
        feature = stub.Do(req)
        print feature

def getpayload(path):
	f = open(path)
	return f.read()

def run():
        channel = grpc.insecure_channel('localhost:9002')
        stub = d20_pb2.HectorStub(channel)
	appname = sys.argv[1]
	method = sys.argv[2]
	httptype = sys.argv[3]
	if httptype == "POST":
		payloadfile = sys.argv[4]
		req = d20_pb2.Request(ApplicationName=appname, ApplicationMethod=method, Method=d20_pb2.RESTMethod.Value(httptype), ApplicationPayload=getpayload(payloadfile))
	elif httptype == "GET":
		filters = sys.argv[4]
		req = d20_pb2.Request(ApplicationName=appname, ApplicationMethod=method, Method=d20_pb2.RESTMethod.Value(httptype), Filter=filters)
	print req
        execute(stub,req)
if __name__ == '__main__':
        run()
