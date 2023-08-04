from queue import Queue
from threading import Thread
import os
import json
import logging
from at.qhandler import QueueHandler
from configparser import ConfigParser
from datetime import datetime
from at.config import ConfigManager
from loadtest.filemgr import FileManager

class QueueManager(Thread):
    
    def __init__(self, cfgmanager,dbmanager,section,start_time, timeleft):
        super(QueueManager, self).__init__()
        #cfgmanager = ConfigManager(pfile)
        self.cfgmanager = cfgmanager
        self.dbmanager = dbmanager 
        self.section = section
        self.sfghosts = cfgmanager.get_sfg_hosts(section)
        self.sfgusers = cfgmanager.get_sfg_users(section)
        self.knownhosts_file = cfgmanager.get_element(section,'known-hosts-file')
        self.parms = cfgmanager.get_section(section)
        self.start_time = start_time
        self.timeleft = timeleft
        self.__create_queues()
        self.__start_listeners()
       

    def run(self):
        filelist = self.__get_list()
        logging.info('sending %d files to %d users in %d hosts' % 
                     (len(filelist), len(self.sfgusers), len(self.sfghosts)))
        fileitem = 0
        for cinfo in self.sfghosts:
            for uinfo in self.sfgusers:
                for q in self.qlist:
                    self.__qput(q, cinfo, uinfo, filelist[fileitem])
                    fileitem += 1
        
    def __create_queues(self):
        self.qlist = []
        num_threads = int(self.parms['thread-count'])
        for i in range(num_threads):
            q = Queue()
            self.qlist.append(q)
            
    
    def __start_listeners(self):
        self.hlist = []
        handlerclass = self.parms['handler-class']
        for q in self.qlist:
            qhandler = QueueHandler(q, handlerclass,self.cfgmanager,self.dbmanager,self.section, self.start_time, self.timeleft)
            qhandler.setDaemon(True)
            qhandler.start()
            self.hlist.append(qhandler)
    
    def shutdown(self):
        for handler in self.hlist:
            handler.join()
            handler.shutdown()

                        
    def __qput(self, q, cinfo, uinfo, fname):
        #print('Ports :', cinfo['port'])
        for port in cinfo['port'].split(','):
            temp = cinfo
            temp['port'] = port
            p = {}
            p['cinfo'] = temp
            p['uinfo'] = uinfo
            p['fname'] = fname
            data = json.dumps(p)
            q.put(data)
            logging.info('sending %s  to %s:%d ' % 
                     (fname, cinfo['host'], cinfo['port']))


    def __get_list2(self):
        flist = []
        filepath = self.parms["data-folder"]
        logging.info('loading files from: ' + filepath)
        for file_list in os.walk(filepath): # Iterates through files & folders inside the rootDir
#            logging.info('found directory: ' + file_list[1])
            for fname in file_list[2]: # We only look for files inside the rootDir
                fpath = filepath+fname
                flist.append(fpath) # List of files with absolute path are appended to the list "flist"
                logging.info('adding to file list '+ fpath)
        return flist

    def __get_list(self):
        flist = []
        filepath = self.parms["data-folder"]
        logging.info('loading files from: ' + filepath)
        fm = FileManager(filepath)
        fm.generate_files()
        fm.reset_counter(int(self.parms["file-limit"]))
        count = 0 
        while True:
            choice = fm.next_file_to_choose()
            if choice!=None:
                #print(choice)
                flist.append(choice)
                count += 1
            else:
                break
        return flist


