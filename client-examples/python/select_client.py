import grpc
import d20_pb2

def execute(stub,req):
        feature = stub.Do(req)
        print feature

def run():
        channel = grpc.insecure_channel('localhost:9001')
        stub = d20_pb2.HectorStub(channel)
	req = d20_pb2.Request(ApplicationName="alltrade", ApplicationMethod="foo", Method=d20_pb2.RESTMethod.Value("GET"), ApplicationPayload="{}")
	print req
        execute(stub,req)
if __name__ == '__main__':
        run()
