from threading import Thread
import json
import time
import os, random, string
import logging
from at.dbutil import DBManager
from datetime import datetime
import traceback
import sys
import threading


FILE_SENDING_LOCK = threading.Lock()

class QueueHandler(Thread):

    def __init__(self, q, handler,cfgmanager,dbmanager,section, start_time, timeleft):
        super(QueueHandler, self).__init__()
        logging.info('Initialized Queue Handler')
        self.lock = threading.Lock()
        self.counter = 1
        self.q = q
        self.handler = handler
        self.known_hosts_file = cfgmanager.get_knownhosts_file(section)
        self.runflag = True
        self.run_id = None
        self.cfgmanager = cfgmanager
        self.testcase = section
        self.dbmanager = dbmanager
        self.start_time = start_time
        self.timeleft = timeleft
        if self.dbmanager == None:
            logging.error('Failed to establish connection to database manager')
        else:
            self.run_id = self.dbmanager.run_id

    def shutdown(self):
        #logging.info('Shutdown called ....')
        self.runflag = False

    def join(self):
        self.q.join()

    def run(self):
        #logging.info('Running queue handler')
        module = __import__(self.handler)
        hclass = getattr(module, self.handler)
        self.client = hclass(self.known_hosts_file)
        #while self.runflag:
        while True:
            t2 = datetime.now()
            delta = t2 - self.start_time
            elapsed =  delta.seconds
            #end_time = datetime.now()
            #elapsed_time = end_time-self.start_time
            data = self.q.get()
            if self.timeleft - elapsed < 0 :
                while data != None:
                    data = self.q.get()
                break
            #logging.info('Running queue handler inside loop')
            #data = self.q.get(10000)
            
            if data == None:
                logging.info('No data found in Queue, quiting ...')
                break
            #    time.sleep(1)
            #    continue
            parms = json.loads(data)
            self.__send_file(parms)
            #self.q.task_done()

    def __send_file(self, parms):
        if self.lock.acquire():
            cinfo = parms['cinfo']
            uinfo = parms['uinfo']
            fname = parms['fname']
            seqno = self.dbmanager.get_next_seqno()
            #remfile = self.__target_name(uinfo['username'],fname,cinfo['prefix'],seqno)
            remfile = self.__target_name(uinfo['username'],fname,seqno)
            print('file=%s, user=%s, endpoint=%s:%s' % (remfile, uinfo['username'], cinfo['host'],cinfo['port']))
            logging.info('file=%s, user=%s, endpoint=%s:%s' % (remfile, uinfo['username'], cinfo['host'],cinfo['port']))
            try:
                start_time = datetime.now()
                self.client.connect(cinfo)
                self.client.login(uinfo)
                self.client.send(fname, remfile)
                end_time = datetime.now()
                delta = end_time - start_time
                time_taken = str(delta)
                filesize = int(os.stat(fname).st_size)
                self.dbmanager.add_info(seqno,fname,remfile,filesize,'%s uploaded user-> %s, endpoint-> %s:%s' %(remfile, uinfo['username'],cinfo['host'],cinfo['port']),'SUCCESS',str(start_time),str(end_time),time_taken,cinfo['host'],cinfo['port'], uinfo['username'])
                self.dbmanager.close_connection()
                logging.info('file uploaded=%s, user=%s, endpoint=%s:%s' % (remfile, uinfo['username'], cinfo['host'], cinfo['port']))
            except Exception as e:
                end_time = datetime.now()
                delta = end_time - start_time
                time_taken = str(delta)
                seqno = self.dbmanager.get_current_seqno()
                filesize = int(os.stat(fname).st_size)
                self.dbmanager.add_info(seqno,fname,remfile,filesize,'file=%s, user=%s, endpoint=%s:%s' %(remfile, uinfo['username'],cinfo['host'],cinfo['port']),'FAILED',str(start_time),str(end_time),time_taken,cinfo['host'],cinfo['port'], uinfo['username'])
                self.dbmanager.close_connection()
                print('WARN:File %s upload failed by %s to %s:%s, REASON:%s' % (remfile, uinfo['username'], cinfo['host'],cinfo['port'],str(e)))
                logging.warn('File %s upload failed by %s to %s:%s, REASON:%s' % (remfile, uinfo['username'], cinfo['host'],cinfo['port'],str(e)))
                logging.error(traceback.format_exc())
            finally:
                if self.client:
                    self.client.close()
            self.q.task_done()
            self.lock.release()
        else:
            print('Lock not acquired:',json.dumps(parms['cinfo']))

    ''' private method '''
    def __target_name(self, username,fname,seqno):
        # Extract only the filename from source to be used as a targetfilename
        #seqno = self.dbmanager.get_next_seqno()
        targetfile = os.path.basename(fname)
        #prefix = host[:3]
        # Only get the last part in the filename after the last . so we get the trancode
        trancode = targetfile.split('.')[-1]
        #run_id =self.dbmanager.get_current_run_id()
        # Create a random 20 character value and prefix it to the trancode
        #targetfile = "%s.%s"% (''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20)),targetfile)
        #targetfile = "%s_%s_%s_%s.%s"% (prefix,str(self.run_id).zfill(5),str(seqno).zfill(6),threading.current_thread().ident,targetfile)

        #targetfile = "JO.%s.%s.%s.PaymentFile%s.%s"% (trancode,''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(17)),username,str(seqno).zfill(6),targetfile)
        arr = fname.split('_')
        #targetfile = '%s_%s%s' % (self.get_formatted_filename(username,trancode,seqno),arr[1],arr[2])
        targetfile = self.get_formatted_filename(username,trancode,seqno)
        fpart = targetfile.split('.')
        targetfile = '.'.join(fpart[:-1])+'.%s%s.%s'%(arr[1],arr[2],fpart[-1])
        return targetfile

    def get_formatted_filename(self,username,trancode,seqno):
        file_format = self.cfgmanager.get_file_format(username)
        tokens = file_format.split('.')
        formatted_file = ""
        try:
            for token in tokens:
                #print("TOKEN : %s" % token)
                if token.find('USERNAME') != -1:
                    #formatted_file = "%s.%s" %(formatted_file,username)
                    formatted_file = self.get_formatted_token(formatted_file,username)
                elif token.find('RANDOM') != -1:                
                    #formatted_file = "%s.%s" % (formatted_file,self.get_random_string(token))
                    formatted_file = self.get_formatted_token(formatted_file,self.get_random_string(token))
                elif (token.find('PREFIX') != -1) and (token.find('SEQ') != -1):
                    temp = token.split('_')
                    #formatted_file = "%s.%s%s" % (formatted_file,temp[1],str(seqno).zfill(6))
                    formatted_file = self.get_formatted_token(formatted_file,"%s%s" % (temp[1],str(seqno).zfill(6)))
                elif token.find('TRANCODE') != -1:
                    #formatted_file = "%s.%s" % (formatted_file,trancode)
                    formatted_file = self.get_formatted_token(formatted_file,trancode)
                else:
                    formatted_file = self.get_formatted_token(formatted_file,token)
        except Exception as e:
            print("Exception raised %s" % str(e))
        #print("FORMATTED_TOKEN %s\n" %formatted_file)
        return formatted_file
            
    def get_formatted_token(self,formatted_file,token):
        if len(formatted_file)>0 :
            return "%s.%s" % (formatted_file,token)
        else:
            return token

    def get_random_string(self,token):
        temp = token.split('_')
        if token.find('PREFIX') != -1:
            return "%s%s"%(temp[1],''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(int(temp[2]))))
        else:
            return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(int(temp[1])))

