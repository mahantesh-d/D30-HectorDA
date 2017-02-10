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
        channel = grpc.insecure_channel('localhost:9001')
        stub = d20_pb2.HectorStub(channel)
	appname = sys.argv[1]
	method = sys.argv[2]
	httptype = sys.argv[3]
	payloadfile = sys.argv[4]
	# req = d20_pb2.Request(ApplicationName="alltrade", ApplicationMethod="foo", Method=d20_pb2.RESTMethod.Value("POST"), ApplicationPayload='{"id": 1,"name": "fgfh"}')


	#req = d20_pb2.Request(ApplicationName="alltrade", ApplicationMethod="foobar", Method=d20_pb2.RESTMethod.Value("POST"), ApplicationPayload='{"email_id" : ["abc@email.com","abcd@email.com"], "dyn": {"a1":"status","b1":"something_else"}}')
	
	req = d20_pb2.Request(ApplicationName=appname, ApplicationMethod=method, Method=d20_pb2.RESTMethod.Value(httptype), ApplicationPayload=getpayload(payloadfile))
	print req
        execute(stub,req)
if __name__ == '__main__':
        run()
