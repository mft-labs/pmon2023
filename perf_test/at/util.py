import logging
import time

def configDefaultLogger(level):
    FORMAT = "%(asctime)-15s %(levelname)-5s %(module)s %(thread)d %(message)s"
    LOG_FILENAME = filename='/opt/B2B/test/perf_test/logs/sftp_test_%s.log' %(str(time.time()))
    logging.basicConfig(filename=LOG_FILENAME,format=FORMAT, level=level)
