from datetime import datetime
from elasticsearch_dsl import DocType, Date, Integer, Keyword, Text, Float
from elasticsearch_dsl.connections import connections
from dateutil.tz import tzlocal
from monitor.utils import AESCipher
import string

class WorkingThread(DocType):
    host = Keyword()
    event_time = Date()
    queue_name = Keyword()
    ins_id = Integer()
    wfid = Integer()
    stepid = Integer()
    expedite = Keyword()
    priority = Keyword()
    msec = Integer()
    wfdname = Keyword()
    wftransporter_status = Text(analyzer='snowball', fields={'raw': Keyword()}) 


    class Meta:
        index = 'working_threads'

    def __str__(self):
        logentry = "WORKING_THREAD~host=%s~event_time=%s~queue_name=%s~ins_id=%s~wfid=%s~stepid=%s~" % (
                        self.host,
                        self.event_time,
                        self.queue_name,
                        self.ins_id,
                        self.wfid,
                        self.stepid
                    )
        logentry = "%sexpedite=%s~priority=%s~msec=%s~wfdname=%s~wftransporter_status=%s~" % (
                        logentry,
                        self.expedite,
                        self.priority,
                        self.msec,
                        self.wfdname,
                        self.wftransporter_status
                    )
        return logentry

    def save(self, ** kwargs):
        return super(WorkingThread, self).save(** kwargs)

class ThreadPoolInfo(DocType):
    host = Keyword()
    event_time = Date()
    queue_name = Keyword()
    min = Integer()
    used = Integer()
    calc = Integer()
    pool = Integer()
    max = Integer()
    queue_depth = Integer()

    class Meta:
        index = 'thread_pool_info'

    def __str__(self):
        logentry = "THREAD_POOL_INFO~host=%s~event_time=%s~queue_name=%s~min=%s~used=%s~calc=%s~" % (
                        self.host,
                        self.event_time,
                        self.queue_name,
                        self.min,
                        self.used,
                        self.calc
                    )
        logentry = "%spool=%s~max=%s~queue_depth=%s~" % (
                        logentry,
                        self.pool,
                        self.max,
                        self.queue_depth
                    )
        return logentry

    def save(self, ** kwargs):
        return super(ThreadPoolInfo, self).save(** kwargs)


class DBPoolInfo(DocType):
    host = Keyword()
    event_time = Date()
    pool_name = Keyword()
    current_size = Integer()
    maximum_size = Integer()
    pool_requests = Integer()
    wait_requests = Integer()
    buffer_requests = Integer()
    delete_requests = Integer()
    bad_item_count = Integer()

    class Meta:
        index = 'db_pool_info'

    def __str__(self):
        logentry = "DB_POOL_INFO~host=%s~event_time=%s~pool_name=%s~current_size=%s~maximuum_size=%s~pool_requests=%s~" % (
                        self.host,
                        self.event_time,
                        self.pool_name,
                        self.current_size,
                        self.maximum_size,
                        self.pool_requests
                    )
        logentry = "%swait_requests=%s~buffer_requests=%s~delete_requests=%s~bad_item_count=%s~" % (
                        logentry,
                        self.wait_requests,
                        self.buffer_requests,
                        self.delete_requests,
                        self.bad_item_count
                    )
        return logentry

    def save(self, ** kwargs):
        return super(DBPoolInfo, self).save(** kwargs)


class HeapMemory(DocType):
    host = Keyword()
    event_time = Date()
    total_memory_gb = Float()
    total_memory_kb = Integer()
    free_memory_gb = Float()
    free_memory_kb = Integer()
    free_memory_pers = Float()
    processors = Integer()


    class Meta:
        index = 'heap_memory'

    def __str__(self):
        logentry = "HEAP_MEMORY~host=%s~event_time=%s~total_memory_gb=%s~total_memory_kb=%s~" % (
                        self.host,
                        self.event_time,
                        self.total_memory_gb,
                        self.total_memory_kb
                    )    
        logentry = "%sfree_memory_gb=%s~free_memory_kb=%s~free_memory_perc=%s~processors=%s~" % (
                        logentry,
                        self.free_memory_gb,
                        self.free_memory_kb,
                        self.free_memory_pers,
                        self.processors
                    )
        return logentry

    def save(self, ** kwargs):
        return super(HeapMemory, self).save(** kwargs)    

class ElasticDslUtil(object):
    def __init__(self, save_to_elasticsearch=True, host=None,username=None,secretkey=None,secword=None,logger=None):
        self.host = host
        self.logger = logger
        self.save_to_elasticsearch = save_to_elasticsearch
        print("Saving to Elastic Search:%s" % self.save_to_elasticsearch)

        if save_to_elasticsearch and username != None:
            secureobj = AESCipher(secretkey)
            password = secureobj.decrypt(secword)    
            connections.create_connection(hosts=[host],http_auth='%s:%s'%(username,password))
        elif save_to_elasticsearch:
            connections.create_connection(hosts=[host])
        if save_to_elasticsearch:
            WorkingThread.init()
            HeapMemory.init()
            ThreadPoolInfo.init()
            DBPoolInfo.init()



    def writeWorkingThread(self,indexname,host,queue_name,ins_id,wfid,stepid,expedite,priority,msec,wfdname,status):
        working_thread = WorkingThread(
            host = host,
            event_time = datetime.now(tzlocal()),
            queue_name = queue_name,
            ins_id = int(ins_id),
            wfid = int(wfid),
            stepid  = int(stepid),
            expedite = expedite,
            priority = priority,
            msec = int(msec),
            wfdname = wfdname,
            wftransporter_status = status
        )
        working_thread.meta.index = "%s_%s" % (indexname,datetime.today().strftime('%Y%m%d'))

        if self.logger != None:
            self.logger.info(str(working_thread))

        if self.save_to_elasticsearch:
            working_thread.save()


    def writeThreadPoolInfo(self,indexname,host,queue_name,min,used,calc,pool,max,queue_depth):
        thread_pool_info = ThreadPoolInfo(
            host = host,
            event_time = datetime.now(tzlocal()),
            queue_name = queue_name,
            min = int(min),
            used = int(used),
            calc  = int(calc),
            pool = int(pool),
            max = int(max),
            queue_depth = int(queue_depth)
        )
        thread_pool_info.meta.index = "%s_%s" % (indexname,datetime.today().strftime('%Y%m%d'))

        if self.logger != None:
            self.logger.info(str(thread_pool_info))

        if self.save_to_elasticsearch:
            #print(thread_pool_info.meta.index)
            thread_pool_info.save()

    def writeDBPoolInfo(self,indexname,host,pool_name,current_size,maximum_size,pool_requests,wait_requests,buffer_requests,delete_requests,bad_item_count):
        pool_name = string.replace(pool_name,'&nbsp;','')
        pool_name = string.replace(pool_name,'&nbsp','')
        db_pool_info = DBPoolInfo(
            host = host,
            event_time = datetime.now(tzlocal()),
            pool_name = pool_name,
            current_size = int(current_size),
            maximum_size = int(maximum_size),
            pool_requests  = int(pool_requests),
            wait_requests = int(wait_requests),
            buffer_requests = int(buffer_requests),
            delete_requests = int(delete_requests),
            bad_item_count = int(bad_item_count)
        )
        db_pool_info.meta.index = "%s_%s" % (indexname,datetime.today().strftime('%Y%m%d'))

        if self.logger != None:
            self.logger.info(str(db_pool_info))

        if self.save_to_elasticsearch:
            #print(thread_pool_info.meta.index)
            db_pool_info.save()


    def writeHeapMemory(self,indexname,host,total_memory_gb,total_memory_kb,free_memory_gb,free_memory_kb,free_memory_pers,processors):
        heap_memory = HeapMemory(
            host = host,
            event_time = datetime.now(tzlocal()),
            total_memory_gb = total_memory_gb,
            total_memory_kb = int(self.cleanup_kb(total_memory_kb)),
            free_memory_gb = free_memory_gb,
            free_memory_kb = int(self.cleanup_kb(free_memory_kb)),
            free_memory_pers = float(self.cleanup_pers(free_memory_pers)),
            processors = processors
        )
        heap_memory.meta.index = "%s_%s" % (indexname,datetime.today().strftime('%Y%m%d'))

        if self.logger != None:
            self.logger.info(str(heap_memory))

        if self.save_to_elasticsearch:
            heap_memory.save()


    def cleanup_kb(self,text):
        return text.replace(" ","")

    def cleanup_pers(self,text):
        return text[:len(text)-1]
