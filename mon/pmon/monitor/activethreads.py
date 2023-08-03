import thread
import requests
from bs4 import BeautifulSoup
import traceback
from datetime import datetime
import calendar

class ActiveThreadsMonitor(object):
    #def __init__(self, logger,eslogger,host_info):
    def __init__(self, logger,esdsl,host_info):
        self.logger = logger
        #self.eslogger = eslogger
        self.esdsl = esdsl
        self.host_info = host_info
        self.securetoken = None
        self.cookies = None
        self.qeueuewatcher_login()
        self.loggedin = False
    
    def qeueuewatcher_login(self):
        logger = self.logger
        try:
            params = {'username':self.host_info['username'],'password':self.host_info['password']}
            resp = requests.post('http://%s:%d/queueWatch/queueWatcher' %(self.host_info['host'],int(self.host_info['port'])),params=params)
            response=resp.text
            pos1 = response.find('securetoken')
            pos2 = response.find("'",pos1)
            self.securetoken = response[pos1:pos2].split('=')[1]
            self.cookies = {'JSESSIONID':resp.cookies['JSESSIONID']}
            self.loggedin = True
            logger.info('Logged In Successfully!')
        except Exception as e:
            logger.error('Exception raised %s' % str(e))
            logger.error(traceback.format_exc())
            self.securetoken = None
            self.cookies = None
            self.loggedin = False

    def queuewatcher_logout(self):
        logger = self.logger
        try:
            resp = requests.post('http://%s:%d/queueWatch/logout.jsp' %(self.host_info['host'],int(self.host_info['port'])),params=None)
            response=resp.text
            self.loggedin = False
            logger.info('Logged out successfully!')
        except Exception as e:
            self.loggedin = False
            logger.info('Exception raised while logout')

    def get_active_threads(self):
        logger = self.logger
        try:
            params = {'securetoken':self.securetoken,'qname':'threads'}
            resp = requests.get('http://%s:%d/queueWatch/queueWatcher'%(self.host_info['host'],int(self.host_info['port'])),params=params,cookies=self.cookies)
            soup = BeautifulSoup(resp.text,"lxml")
            table = soup.find_all('table')[0]
            list = {}
            row = 0
            for tr in table.find_all('tr'):
                columns = []
                for td in tr.find_all('td'):
                    columns.append(td.text)
                list[str(row)] = columns
                row = row + 1

            table2 = soup.find_all('table')[1]
            list2 = {}
            row = 0
            for tr in table2.find_all('tr'):
                columns = []
                for td in tr.find_all('td'):
                    columns.append(td.text)
                list2[str(row)] = columns
                row = row + 1
            return {'allqueues':list,'workingthreads':list2}
        except Exception as e:
            logger.error(traceback.format_exc())
        return None

    def get_working_threads(self,indexname):
        logger = self.logger
        results = self.get_active_threads()
        if results != None:
            workingthreads = results['workingthreads']
            row = 1
            logger.info("HOST:%s --> No. of entries in working threads is %d" % (self.host_info['host'],len(workingthreads)))
            while row < len(workingthreads):
                info = workingthreads[str(row)]
                wftstatus = "wfTransporter Status: "
                t = 9
                while t <=15:
                    if t != 15:
                        wftstatus = "%s%s, " %(wftstatus,info[t])
                    else:
                        wftstatus = "%s%s" %(wftstatus,info[t])
                    t = t+1
                logger.info("HOST:%s -->(Working Threads) QueueName:%s, insID:%s, WFID:%s, StepId:%s, expedite:%s, Priority:%s, msec:%s, wfdname:%s, %s" % (self.host_info['host'],info[0],info[1],info[2],info[3],info[4],info[5],info[6],info[7],wftstatus))
                d=datetime.utcnow()
                timestamp=calendar.timegm(d.utctimetuple())
                self.esdsl.writeWorkingThread(indexname,self.host_info['host'],info[0],info[1],info[2],info[3],info[4],info[5],info[6],info[7],wftstatus)
                row = row + 1


    def get_heap_memory(self,indexname):
        """
        View Heap Memory Level
        """
        logger = self.logger
        params = {'securetoken':self.securetoken,'qname':'ListMemory'}
        resp = requests.get('http://%s:%d/queueWatch/queueWatcher'%(self.host_info['host'],int(self.host_info['port'])),params=params,cookies=self.cookies)
        soup2 = BeautifulSoup(resp.text,"lxml")
        table3 = soup2.find_all('table')[0]
        memorylist = {}
        row_count = 0
        for tr in table3.find_all('tr'):
            columns = []
            if row_count == 0:
                for th in tr.find_all('th'):
                    columns.append(th.text)
            else:
                th = tr.find_all('th') [0]
                columns.append(th.text)
            for td in tr.find_all('td'):
                columns.append(td.text)
            if row_count != 0:
                th_list = tr.find_all('th')
                if len(th_list) > 1:
                    th = th_list[1]
                    columns.append(th.text)
            memorylist[str(row_count)] = columns
            row_count = row_count + 1

        logger.info('HOST:%s -->(Heap Memory) Total Memory: %s GB, %s KB' % (self.host_info['host'],memorylist['1'][1],memorylist['1'][2]))
        logger.info('HOST:%s -->(Heap Memory) Free Memory: %s GB, %s KB, %s' % (self.host_info['host'],memorylist['2'][1],memorylist['2'][2],memorylist['2'][3]))
        logger.info('HOST:%s -->(Heap Memory) Processors: %s' % (self.host_info['host'],memorylist['3'][1]))
        d=datetime.utcnow()
        timestamp=calendar.timegm(d.utctimetuple())
        self.esdsl.writeHeapMemory(
             indexname,
             self.host_info['host'],
             memorylist['1'][1],
             memorylist['1'][2],
             memorylist['2'][1],
             memorylist['2'][2],
             memorylist['2'][3],
             memorylist['3'][1]
         );
        
