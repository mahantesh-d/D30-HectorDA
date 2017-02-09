import grpc
import d20_pb2

def execute(stub,req):
        feature = stub.Do(req)
        print feature

def run():
        channel = grpc.insecure_channel('localhost:9001')
        stub = d20_pb2.HectorStub(channel)
	# req = d20_pb2.Request(ApplicationName="alltrade", ApplicationMethod="foo", Method=d20_pb2.RESTMethod.Value("POST"), ApplicationPayload='{"id": 1,"name": "fgfh"}')
	req = d20_pb2.Request(ApplicationName="alltrade", ApplicationMethod="foobar", Method=d20_pb2.RESTMethod.Value("POST"), ApplicationPayload='{"email_id" : ["abc@email.com","abcd@email.com"], "dyn": {"a1":"status","b1":"something_else"}}')
	print req
        execute(stub,req)
if __name__ == '__main__':
        run()
