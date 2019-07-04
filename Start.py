from concurrent import futures
import grpc
import time
import config
import GRPCServer_impl
import GRPCServer_pb2_grpc

def server_start():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers = 50))
    GRPCServer_pb2_grpc.add_GRPCServerServicer_to_server(GRPCServer_impl.GRPCServer_impl(), server)
    server.add_insecure_port('0.0.0.0:' + config.port)
    server.start()
    try:
        while True:
            time.sleep(config.one_day_in_seconds)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    if not os.path.exists(config.efs_file_root):
        os.mkdir(config.efs_file_root)
    server_start()
