#
"""
QueueWatcher Monitor
"""
import os
import sys
import argparse
import yaml
from monitor.activethreads import ActiveThreadsMonitor
from monitor.utils import ConfigManager
from monitor.utils import OurLogger
from monitor.utils import ESLogger
from monitor.utils import AESCipher
import time
import traceback
from esutils.utils import ElasticDslUtil

def main():
    """
    Main Entry Point of QueueWatcher Monitor
    """
    parser = argparse.ArgumentParser(description='QueueWatcher Monitor')
    parser.add_argument('-cf','--config-file', help='Config file path',required=True)
    args = parser.parse_args()

    config = None
    if args.config_file:
        config = ConfigManager(args.config_file)
        print('App Name:%s' % config.get_value('appname'))
        logger_info = config.get_value('logger')
        elk_info = config.get_value('elk')

        ourlogger = None
        if logger_info != None:
            ourlogger = OurLogger(logger_info['location'],logger_info['logname'],logger_info['filename']).getLogger()
        
        #eslogger = ESLogger(elk_info['index_name'],host=elk_info['host'],port=int(elk_info['port']))
        esdsl = None
        if elk_info != None:
            if 'user' in elk_info:
                esdsl = ElasticDslUtil( host=elk_info['host'],
                                        username=elk_info['user'],
                                        secretkey=elk_info['secretkey'],
                                        secword=elk_info['password'],logger=ourlogger)
            else:
                esdsl = ElasticDslUtil(host=elk_info['host'],logger=ourlogger)
        else:
            print('ELK Configuaration not defiled!')
            esdsl = ElasticDslUtil(save_to_elasticsearch=False,logger=ourlogger)

        #ourlogger = LogstashLogger("localhost",5044,"queuewather-monitor").getLogger()
        nodes = config.get_value('nodelist')
        while True:
            for node in nodes:
                try:
                    if ourlogger != None:
                        ourlogger.info('Going to get info of node:%s' % node)
                    host_info = config.get_value(node)
                    secureobj = AESCipher(host_info['secretkey'])    
                    #host_info['password'] = secureobj.decrypt(host_info['password'])
                    host_info2 = {'host':host_info['host'],'port':host_info['port'],'username':host_info['username'],'password':secureobj.decrypt(host_info['password'])}
                    
                    """
                    ourlogger.info("Host:%s, Port:%d, Username:%s, Password:%s" % (
                        host_info['host'],
                        int(host_info['port']),
                        host_info['username'],
                        host_info['password']
                    ))
                    """
                    #qwmonitor = ActiveThreadsMonitor(ourlogger,eslogger,host_info2)
                    working_threads_index = 'working_threads'
                    heap_memory_index = 'heap_memory'
                    thread_pool_info_index = 'thread_pool_info'
                    db_pool_info_index = 'db_pool_info'
                    if 'index' in host_info:
                        indices = host_info['index']
                        if 'heap_memory' in indices:
                            heap_memory_index = indices['heap_memory']
                        if 'working_threads' in indices:
                            working_threads_index = indices['working_threads']
                        if 'thread_pool_info' in indices:
                            thread_pool_info_index = indices['thread_pool_info']
                        if 'db_pool_info' in indices:
                            db_pool_info_index = indices['db_pool_info']
                    #print(thread_pool_info_index)
                    qwmonitor = ActiveThreadsMonitor(ourlogger,esdsl,host_info2)
                    qwmonitor.get_thread_pool_info(thread_pool_info_index)
                    qwmonitor.get_working_threads(working_threads_index)
                    qwmonitor.get_heap_memory(heap_memory_index)
                    qwmonitor.get_list_db_pools(db_pool_info_index)
                    qwmonitor.queuewatcher_logout()

                except Exception as e:
                    if ourlogger != None:
                        ourlogger.error(traceback.format_exc())
            if config.get_value('settings') != None:
                settings = config.get_value('settings')
                time.sleep(float(settings['delay']))
            else:
                time.sleep(0.10)
            #time.sleep(60)
            #ourlogger.info("Using Dict --> Host: %s" % config.get_dictitem(node,'host'))
    else:
        pass


if __name__ == "__main__":
    main()
