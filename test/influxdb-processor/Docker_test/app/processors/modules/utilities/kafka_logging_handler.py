import logging,sys
import socket

class KafkaLoggingHandler(logging.Handler):
    def __init__(self, producer, log_topic):
        super().__init__()
        self.producer = producer
        self.log_topic = log_topic

    def emit(self, record):
        try:
            msg = {
                'msg': str(self.format(record)),
                'hostname': socket.gethostname()
            }
            self.producer.send(self.log_topic, msg)
            self.producer.flush(timeout=1.0)
        except:
            import traceback
            ei = sys.exc_info()
            traceback.print_exception(ei[0], ei[1], ei[2], None, sys.stderr)
            del ei