

from stats.collect_stats import ElkCollectStats
from monitor.utils import ConfigManager
import argparse
import yaml
import time
from monitor.utils import OurLogger

def main():
    """
    Main Entry Point of Stats Collector
    """
    parser = argparse.ArgumentParser(description='Stats Collector')
    parser.add_argument('-cf','--config-file', help='Config file path',required=True)
    args = parser.parse_args()

    config = None
    if args.config_file:
        config = ConfigManager(args.config_file)
        #indices = {}
        #indices['DiskUsageStats']='disk_usage_stats'
        indices = config.get_value('index')
        elk_info = config.get_value('elk')
        settings = config.get_value('settings')

        logger_info = config.get_value('logger')
        ourlogger = None
        if logger_info != None:
            ourlogger = OurLogger(logger_info['location'],logger_info['logname'],logger_info['filename']).getLogger()

        esdsl = None
        processfile = None
        pidlist = None
        processfiles = config.get_value('processfiles')
        if processfiles != None:
            pidlist = []
            for processfile_element in processfiles:
                process_file = config.get_value(processfile_element)
                print('Getting PID from %s' % process_file)
                f = open(process_file,'r')
                pid = int(f.readline())
                f.close()
                pidlist.append(pid)

        if (elk_info != None) and ('user' in elk_info):
            esdsl = ElkCollectStats( host=elk_info['host'],
                                    username=elk_info['user'],
                                    secretkey=elk_info['secretkey'],
                                    secword=elk_info['password'],
                                    pidlist = pidlist,logger=ourlogger)
        elif elk_info !=None:
            esdsl = ElkCollectStats(host=elk_info['host'],index_names=indices,pidlist=pidlist,logger=ourlogger)
        else:
            esdsl = ElkCollectStats(host=None,index_names=indices,pidlist=pidlist,logger=ourlogger)

        while True:
            if ourlogger != None:
                ourlogger.info('Capturing Stats ...')
            esdsl.save_stats()
            if 'delay' in settings:
                time.sleep(int(settings['delay']))
            else:
                time.sleep(60)
        if ourlogger != None:
            ourlogger.info('End of Capturing Stats ...')
if __name__ == "__main__":
    main()