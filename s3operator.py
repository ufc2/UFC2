import threading
import global_variables
import config

sem = threading.Semaphore(config.s3_max_threads)

class operator(threading.Thread):

    def __init__(self, args):
        threading.Thread.__init__(self)
        self.args = args

    def run(self):
        sem.acquire()
        if self.args['operation'] == 'r':
            self.args['data'] = global_variables._s3connector.read(self.args['key'])
        elif self.args['operation'] == 'w':
            global_variables._s3connector.write(self.args['key'], self.args['data'])
        else:
            global_variables._s3connector.delete(self.args['key'])
        sem.release()
