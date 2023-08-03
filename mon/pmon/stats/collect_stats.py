from __future__ import print_function
import socket
import sys
import os
import psutil
from datetime import datetime
from elasticsearch_dsl import DocType, Date, Integer, Keyword, Text, Float, Nested, InnerObjectWrapper, Long
from elasticsearch_dsl.connections import connections
from dateutil.tz import tzlocal
from monitor.utils import AESCipher
import socket
import time
import traceback

class VirtualMemory(DocType):
    host = Keyword()
    event_time = Date()
    total_memory = Float()
    total_memory_formatted = Text(analyzer='snowball')
    available_memory = Float()
    available_memory_formatted = Text(analyzer='snowball')
    used_memory = Float()
    used_memory_formatted = Text(analyzer='snowball')
    free_memory = Float()
    free_memory_formatted = Text(analyzer='snowball')
    percent_used = Float()

    class Meta:
        index = 'virtual_memory'

    def __str__(self):
        return """VIRTUALMEMORY~host=%s~event_time=%s~total_memory=%s~total_memory_formatted=%s~available_memory=%s~available_memory_formatted=%s~used_memory=%s~used_memory_formatted=%s~free_memory=%s~free_memory_formatted=%s~percent_used=%s~""" % (
                   self.host,
                   self.event_time,
                   self.total_memory,
                   self.total_memory_formatted,
                   self.available_memory,
                   self.available_memory_formatted,
                   self.used_memory,
                   self.used_memory_formatted,
                   self.free_memory,
                   self.free_memory_formatted,
                   self.percent_used
               )
    
    def save(self, ** kwargs):
        return super(VirtualMemory, self).save(** kwargs)

class SwapMemory(DocType):
    host = Keyword()
    event_time = Date()
    total_memory = Float()
    total_memory_formatted = Text(analyzer='snowball')
    used_memory = Float()
    used_memory_formatted = Text(analyzer='snowball')
    free_memory = Float()
    free_memory_formatted = Text(analyzer='snowball')
    percent_used = Float()
    sin_memory = Float()
    sin_memory_formatted = Text(analyzer='snowball')
    sout_memory = Float()
    sout_memory_formatted = Text(analyzer='snowball')

    class Meta:
        index = 'swap_memory'

    def __str__(self):
        return "SWAPMEMORY~host=%s~event_time=%s~total_memory=%s~total_memory_formatted=%s~used_memory=%s~used_memory_formatted=%s~free_memory=%s~free_memory_formatted=%s~percent_used=%s~sin_memory=%s~sin_memory_formatted=%s~sout_memory=%s~sout_memory_formatted=%s" % (
            self.host,
            self.event_time,
            self.total_memory,
            self.total_memory_formatted,
            self.used_memory,
            self.used_memory_formatted,
            self.free_memory,
            self.free_memory_formatted,
            self.percent_used,
            self.sin_memory,
            self.sin_memory_formatted,
            self.sout_memory,
            self.sout_memory_formatted
        )
    
    def save(self, ** kwargs):
        return super(SwapMemory, self).save(** kwargs)




class ProcessCpuInfoStats(DocType):
    host = Keyword()
    event_time = Date()
    name = Text(analyzer='snowball')
    cpu_times = Float()
    cpu_percent = Float()
    create_time = Date()
    ppid = Integer()
    pid = Integer()
    status = Text(analyzer='snowball')
    username = Text(analyzer='snowball')
    cwd = Text(analyzer='snowball') 
    parent_pid = Integer()
    parent_name = Text(analyzer='snowball')
    executable = Text(analyzer='snowball')
    commandline = Text(analyzer='snowball')

    class Meta:
        index = 'process_cpu_info'

    def __str__(self):
        return "PROCESS_CPU_INFO_STATS~host=%s~event_time=%s~name=%s~cpu_times=%s~cpu_percent=%s~create_time=%s~ppid=%s~pid=%s~status=%s~username=%s~cwd=%s~parent_pid=%s~parent_name=%s~executable=%s~commandline=%s~" % (
            self.host,
            self.event_time,
            self.name,
            self.cpu_times,
            self.cpu_percent,
            self.create_time,
            self.ppid,
            self.pid,
            self.status,
            self.username,
            self.cwd,
            self.parent_pid,
            self.parent_name,
            self.executable,
            self.commandline
        )
    
    def save(self, ** kwargs):
        return super(ProcessCpuInfoStats, self).save(** kwargs)

class ProcessMemoryInfoStats(DocType):
    host = Keyword()
    event_time = Date()
    rss = Long()
    vms = Long()
    shared = Long()
    text = Long()
    lib = Long()
    data = Long()
    dirty = Integer()
    uss = Long()
    pss = Long()
    swap = Long()
    pid = Integer()

    class Meta:
        index = 'process_memory_info'

    def __str__(self):
        return "PROCESS_MEMORY_INFO~host=%s~event_time=%s~rss=%s~vms=%s~shared=%s~text=%s~lib=%s~data=%s~dirty=%s~uss=%s~pss=%s~swap=%s~pid=%s~" % (
            self.host,
            self.event_time,
            self.rss,
            self.vms,
            self.shared,
            self.text,
            self.lib,
            self.data,
            self.dirty,
            self.uss,
            self.pss,
            self.swap,
            self.pid
        )
    
    def save(self, ** kwargs):
        return super(ProcessMemoryInfoStats, self).save(** kwargs)


class ProcessConnectionsStats(DocType):
    host = Keyword()
    event_time = Date()
    fd = Integer()
    local_ip = Text(analyzer='snowball')
    local_port = Integer()
    remote_ip = Text(analyzer='snowball')
    remote_port = Integer()
    status = Text(analyzer='snowball')
    pid = Integer()
    class Meta:
        index = 'process_connections'
    
    def __str__(self):
        return "PROCESS_CONNECTIONS~host=%s~event_time=%s~fd=%s~local_ip=%s~local_port=%s~remote_ip=%s~remote_port=%s~status=%s~pid=%s~" % (
            self.host,
            self.event_time,
            self.fd,
            self.local_ip,
            self.local_port,
            self.remote_ip,
            self.remote_port,
            self.status,
            self.pid
        )

    def save(self, ** kwargs):
        return super(ProcessConnectionsStats, self).save(** kwargs)

class CpuPercentageIndividual(InnerObjectWrapper):
    pass


class CpuUsage(DocType):
    host = Keyword()
    event_time = Date()
    cpu_percentage_overall = Float()
    cpu_percentage_individual = Text(analyzer='snowball',fields={'raw': Keyword()})
    cpu_percentage = Nested(
                        doc_class=CpuPercentageIndividual,
                        properties={
                        'percentage': Float()
                        }
                    )
    user_percentage = Float()
    system_percentage = Float()
    idle_percentage = Float()
    interrupt_percentage = Float()
    dpc_percentage = Float()

    class Meta:
        index = 'cpu_usage'
    
    def __str__(self):
        logentry = "CPU_USAGE~host=%s~event_time=%s~" % (self.host,self.event_time)
        logentry = "%scpu_percentage_overall=%s~cpu_percentage_individual=%s~" % (
                      logentry,self.cpu_percentage_overall,self.cpu_percentage_individual  
                    )
        for perc in self.cpu_percentage:
            logentry = "%scpu_percentage_%d=%s~" % (logentry,perc['index'],perc['percentage'])
        logentry = "%suser_percentage=%s~system_percentage=%s~idle_percentage=%s~" % (
                        logentry,self.user_percentage,self.system_percentage,self.idle_percentage
                    )
        logentry = "%sinterrupt_percentage=%s~dpc_percentage=%s~" % (logentry,self.interrupt_percentage,self.dpc_percentage)
        return logentry

    def add_item(self, index,percentage):
        self.cpu_percentage.append({'index':index,'percentage':percentage})

    def save(self, ** kwargs):
        return super(CpuUsage, self).save(** kwargs)


class NetIOStats(DocType):
    host = Keyword()
    event_time = Date()
    bytes_sent = Float()
    bytes_recv = Float()
    packets_sent = Float()
    packets_recv = Float()
    errin = Float()
    errout = Float()
    dropin = Float()
    dropout = Float()

    class Meta:
        index = 'net_io_stats'
    
    def __str__(self):
        logentry = "NET_IO_STATS~host=%s~event_time=%s~bytes_sent=%s~bytes_recv=%s~packets_sent=%s~packets_recv=%s~" % (
                        self.host,
                        self.event_time,
                        self.bytes_sent,
                        self.bytes_recv,
                        self.packets_sent,
                        self.packets_sent
                    )
        logentry = "%serrin=%s~errout=%s~dropin=%s~dropout=%s~" % (logentry,self.errin,self.errout,self.dropin,self.dropout)
        return logentry

    def save(self, ** kwargs):
        return super(NetIOStats, self).save(** kwargs)


class DiskIOStats(DocType):
    host = Keyword()
    event_time = Date()
    device_name = Text(analyzer='snowball')
    read_count = Long()
    write_count = Long()
    read_bytes = Long()
    write_bytes = Long()
    read_time = Long()
    write_time = Long()

    class Meta:
        index = 'disk_io_stats'

    def __str__(self):
        logentry = "DISK_IO_STATS~host=%s~event_time=%s~device_name=%s~read_count=%s~write_count=%s~" %(
                        self.host,
                        self.event_time,
                        self.device_name,
                        self.read_count,
                        self.write_count
                    )
        logentry = "%sread_bytes=%s~write_bytes=%s~read_time=%s~write_time=%s" %(
                        logentry,self.read_bytes,self.write_bytes,self.read_time,self.write_time
                    )
        return logentry

    def save(self, ** kwargs):
        return super(DiskIOStats, self).save(** kwargs)


class DiskUsageStats(DocType):
    host = Keyword()
    event_time = Date()
    device = Text(analyzer='snowball')
    mountpoint = Text(analyzer='snowball')
    fstype = Text(analyzer='snowball')
    opts = Text(analyzer='snowball')
    total = Long()
    used = Long()
    free = Long()
    percent = Float()
    
    class Meta:
        index = 'disk_usage_stats'
    
    def __str__(self):
        logentry = "DISK_USAGE_STATS~host=%s~event_time=%s~" % (self.host,self.event_time)
        logentry = "%sdevice=%s~mountpoint=%s~fstype=%s~opts=%s~total=%s~used=%s~free=%s~percent=%s~"%(
                        logentry,
                        self.device,
                        self.mountpoint,
                        self.fstype,
                        self.opts,
                        self.total,
                        self.used,
                        self.free,
                        self.percent
                    )
        return logentry

    def save(self, ** kwargs):
        return super(DiskUsageStats, self).save(** kwargs)

class ElkCollectStats(object):
    def __init__(self, index_names=None,host=None,username=None,secretkey=None,secword=None,pidlist=None,logger=None):
        self.host = host
        self.index_names = index_names
        self.pidlist = pidlist
        self.logger = logger
        self.save_to_elasticsearch = False
        if host!=None and username != None:
            secureobj = AESCipher(secretkey)
            password = secureobj.decrypt(secword)    
            connections.create_connection(hosts=[host],http_auth='%s:%s'%(username,password))
            self.save_to_elasticsearch = True
        elif host != None:
            connections.create_connection(hosts=[host])
            self.save_to_elasticsearch = True
        if self.save_to_elasticsearch:
            VirtualMemory.init()
            SwapMemory.init()
            CpuUsage.init()
            NetIOStats.init()
            DiskIOStats.init()
            DiskUsageStats.init()
            ProcessMemoryInfoStats.init()

    def bytes2human(self, n):
        # >>> bytes2human(10000)
        # '9.8K'
        # >>> bytes2human(100001221)
        # '95.4M'
        symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
        prefix = {}
        for i, s in enumerate(symbols):
            prefix[s] = 1 << (i + 1) * 10
        for s in reversed(symbols):
            if n >= prefix[s]:
                value = float(n) / prefix[s]
                return ('%.1f'%value, '%s'%s) 
        return ("%s" % n,"B")

    def pprint_ntuple(self, nt):
        keys = []
        info = {}
        for name in nt._fields:
            value = getattr(nt, name)
            keys.append(name)
            if name != 'percent':
                value2 = self.bytes2human(value)
                info[name] = value2[0]
                info["%s_%s" % (name,"unit")] = value2[1]
                info["%s_%s" % (name,"memory")] = value
                #print('%-10s : %6s %1s' % (name.capitalize(), value2[0],value2[1]))
                #print('%-10s : %6s' % (name.capitalize(), value))
            else:
                info[name] = value
                info["%s_%s" % (name,"unit")] = "%"
                info["%s_%s" % (name,"memory")] = value
                #print('%-10s : %6s' % (name.capitalize(), value))
        return (keys,info)
        

    def collect_memory_stats(self):
        print('Virtual Memory')
        keys,info = self.pprint_ntuple(psutil.virtual_memory())
        for key in keys:
            print("%-10s : %20s : %7s" % (key.capitalize(),info["%s_%s" %(key,"memory")],"%s %s" %(info[key],info["%s_%s" %(key,"unit")])))
        print('Swap Memory')
        keys2,info2 = self.pprint_ntuple(psutil.swap_memory())
        for key in keys2:
            print("%-10s : %20s : %7s" % (key.capitalize(),info2["%s_%s" %(key,"memory")],"%s %s" %(info2[key],info2["%s_%s" %(key,"unit")])))

    def save_stats(self):
        try:
            self.save_virtual_memory()
        except Exception as e:
            print(traceback.format_exc())

        try:
            self.save_swap_memory()        
        except Exception as e:
            print(traceback.format_exc())

        try:
            self.save_cpu_usage()        
        except Exception as e:
            print(traceback.format_exc())

        try:
            self.save_net_io_stats()
        except Exception as e:
            print(traceback.format_exc())

        try:
            self.save_disk_io_stats()
        except Exception as e:
            print(traceback.format_exc())
        
        try:
            self.save_disk_usage_stats()
        except Exception as e:
            print(traceback.format_exc())

        pidlist = []
        """
        current_process = psutil.Process()
        pidlist.append(current_process)
        current_process_children = current_process.children(recursive=True)
        for pid in current_process_children:
            pidlist.append(pid)
        """
        for pid in self.pidlist:
            pidlist.append(pid)
            p =  psutil.Process(pid=pid)
            p_children = p.children(recursive=True)
            for child in p_children:
                pidlist.append(child.pid)

        try:
            self.save_current_process_cpu_info()
        except Exception as e:
            print(traceback.format_exc())

        try:
            self.save_current_process_memory_info()
        except Exception as e:
            print(traceback.format_exc())

        for pid in pidlist:
            try:
                self.save_process_cpu_info(pid)
            except Exception as e:
                print(traceback.format_exc())

            try:
                self.save_process_memory_info(pid)
            except Exception as e:
                print(traceback.format_exc())

            try:
                self.save_process_connection_info(pid)
            except Exception as e:
                print(traceback.format_exc())


    def format_index(self,index_name):
        return "%s_%s" % (index_name,datetime.today().strftime("%b%Y").lower())
    
    def save_virtual_memory(self):
        stats = VirtualMemory()
        stats.host = socket.gethostname()
        keys,info = self.pprint_ntuple(psutil.virtual_memory())
        stats.total_memory = float(info['total_memory'])
        stats.total_memory_formatted  = "%s %s" %(info['total'], info['total_unit'])
        #print("%s -> %s" %(stats.total_memory,stats.total_memory_formatted))
        stats.available_memory = float(info['available_memory'])
        stats.available_memory_formatted  = "%s %s" %(info['available'], info['available_unit'])
        #print("%s -> %s" %(stats.available_memory,stats.available_memory_formatted))
        stats.used_memory = float(info['used_memory'])
        stats.used_memory_formatted  = "%s %s" %(info['used'], info['used_unit'])
        #print("%s -> %s" %(stats.used_memory,stats.used_memory_formatted))
        stats.free_memory = float(info['free_memory'])
        stats.free_memory_formatted  = "%s %s" %(info['free'], info['free_unit'])
        #print("%s -> %s" %(stats.free_memory,stats.free_memory_formatted))
        stats.percent_used = float(info['percent'])
        #stats.percent_used_formatted  = "%s %s" %(info['percent_memory'], info['percent_unit'])
        #print("%s" %(stats.percent_used))
        stats.host = socket.gethostname()
        stats.event_time =  datetime.now(tzlocal())
        index_name = 'demo_virtual_memory_stats'
        if self.index_names != None:
            if 'VirtualMemory' in self.index_names:
                index_name = self.index_names['VirtualMemory']
        stats.meta.index = self.format_index(index_name)
        if self.logger != None:
            self.logger.info(str(stats))
        if self.save_to_elasticsearch:
            stats.save()

    def save_swap_memory(self):
        stats = SwapMemory()
        keys,info = self.pprint_ntuple(psutil.swap_memory())
        stats.total_memory = float(info['total_memory'])
        stats.total_memory_formatted  = "%s %s" %(info['total'], info['total_unit'])
        #print("%s -> %s" %(stats.total_memory,stats.total_memory_formatted))
        stats.used_memory = float(info['used_memory'])
        stats.used_memory_formatted  = "%s %s" %(info['used'], info['used_unit'])
        #print("%s -> %s" %(stats.used_memory,stats.used_memory_formatted))
        stats.free_memory = float(info['free_memory'])
        stats.free_memory_formatted  = "%s %s" %(info['free'], info['free_unit'])
        #print("%s -> %s" %(stats.free_memory,stats.free_memory_formatted))
        stats.percent_used = float(info['percent'])
        #stats.percent_used_formatted  = "%s %s" %(info['percent_memory'], info['percent_unit'])
        #print("%s" %(stats.percent_used))
        stats.sin_memory = float(info['sin_memory'])
        stats.sin_memory_formatted  = "%s %s" %(info['sin'], info['sin_unit'])
        #print("%s -> %s" %(stats.sin_memory,stats.sin_memory_formatted))
        stats.sout_memory = float(info['sout_memory'])
        stats.sout_memory_formatted  = "%s %s" %(info['sout'], info['sout_unit'])
        #print("%s -> %s" %(stats.sout_memory,stats.sout_memory_formatted))
        stats.host = socket.gethostname()
        stats.event_time =  datetime.now(tzlocal())
        #stats.meta.index = 'dev4_swap_memory'
        index_name = 'demo_swap_memory_stats'
        if self.index_names != None:
            if 'SwapMemory' in self.index_names:
                index_name = self.index_names['SwapMemory']
        stats.meta.index = self.format_index(index_name)
        if self.logger != None:
            self.logger.info(str(stats))
        if self.save_to_elasticsearch:
            stats.save()

    def save_cpu_usage(self):
        stats = CpuUsage()
        stats.cpu_percentage_overall = psutil.cpu_percent()
        cpu_usage_text = None
        cpu_percentage_list = psutil.cpu_percent(percpu=True)
        #stats.cpu_percentage = cpu_percentage_list
        indx = 0
        for cpu_per in cpu_percentage_list:
            if cpu_usage_text == None:
                cpu_usage_text = "%s" % cpu_per
            else:
                cpu_usage_text = "%s, %s" % (cpu_usage_text, cpu_per)
            indx = indx + 1
            stats.add_item(indx,cpu_per)
        stats.cpu_percentage_individual = cpu_usage_text
        cpu_times = psutil.cpu_times_percent()
        stats.user_percentage = cpu_times.user
        stats.system_percentage = cpu_times.system
        stats.idle_percentage = cpu_times.idle
        stats.interrupt_percentage = cpu_times.interrupt
        stats.dpc_percentage = cpu_times.dpc
        stats.host = socket.gethostname()
        stats.event_time =  datetime.now(tzlocal())
        #stats.meta.index = 'dev5_cpu_usage'
        index_name = 'cpu_usage_stats'
        if self.index_names != None:
            if 'CpuUsage' in self.index_names:
                index_name = self.index_names['CpuUsage']
        stats.meta.index = self.format_index(index_name)
        if self.logger != None:
            self.logger.info(str(stats))
        if self.save_to_elasticsearch:
            stats.save()

    def save_net_io_stats(self):
        stats = NetIOStats()
        stats.host = socket.gethostname()
        stats.event_time =  datetime.now(tzlocal())
        net_io_counter = psutil.net_io_counters()
        stats.bytes_sent = net_io_counter.bytes_sent
        stats.bytes_recv = net_io_counter.bytes_recv
        stats.packets_sent = net_io_counter.packets_sent
        stats.packets_recv = net_io_counter.packets_recv
        stats.errin = net_io_counter.errin
        stats.errout = net_io_counter.errout
        stats.dropin = net_io_counter.dropin
        stats.dropout = net_io_counter.dropout
        stats.host = socket.gethostname()
        stats.event_time =  datetime.now(tzlocal())
        #stats.meta.index = 'dev5_net_io_stats'
        index_name = 'demo_net_io_stats'
        if self.index_names != None:
            if 'NetIOStats' in self.index_names:
                index_name = self.index_names['NetIOStats']
        stats.meta.index = self.format_index(index_name)
        if self.logger != None:
            self.logger.info(str(stats))
        if self.save_to_elasticsearch:
            stats.save()

    def save_disk_io_stats(self):
        disk_io_counters = psutil.disk_io_counters(perdisk=True)
        for key in disk_io_counters.keys():
            disk_io_counter = disk_io_counters[key]
            stats = DiskIOStats()
            stats.device_name = key
            stats.read_count = disk_io_counter.read_count
            stats.read_bytes = disk_io_counter.read_bytes
            stats.read_time = disk_io_counter.read_time
            stats.write_count = disk_io_counter.write_count
            stats.write_bytes = disk_io_counter.write_bytes
            stats.read_time = disk_io_counter.write_time
            stats.host = socket.gethostname()
            stats.event_time =  datetime.now(tzlocal())
            #stats.meta.index = 'dev5_disk_io_stats'
            index_name = 'demo_disk_io_stats'
            if self.index_names != None:
                if 'DiskIOStats' in self.index_names:
                    index_name = self.index_names['DiskIOStats']
            stats.meta.index = self.format_index(index_name)
            if self.logger != None:
                self.logger.info(str(stats))
            if self.save_to_elasticsearch:
                stats.save()

    def save_disk_usage_stats(self):
        mounting_points = psutil.disk_partitions()
        for mounting_point in mounting_points:
            disk_usage = psutil.disk_usage(mounting_point.mountpoint)
            stats = DiskUsageStats()
            stats.device = mounting_point.device
            stats.mountpoint = mounting_point.mountpoint
            stats.fstype = mounting_point.fstype
            stats.opts = mounting_point.opts
            stats.total = disk_usage.total
            stats.used = disk_usage.used
            stats.free = disk_usage.free
            stats.percent = disk_usage.percent
            stats.host = socket.gethostname()
            stats.event_time =  datetime.now(tzlocal())
            index_name = 'dev5_disk_usage_stats'
            if self.index_names != None:
                if 'DiskUsageStats' in self.index_names:
                    index_name = self.index_names['DiskUsageStats']
            stats.meta.index = self.format_index(index_name)
            if self.logger != None:
                self.logger.info(str(stats))
            if self.save_to_elasticsearch:
                stats.save()

    def save_current_process_cpu_info(self):
        current_process = psutil.Process()
        with current_process.oneshot():
            process_cpu_info = ProcessCpuInfoStats()
            process_cpu_info.pid = current_process.pid
            process_cpu_info.name = current_process.name()
            process_cpu_info.cpu_times = current_process.cpu_times()
            process_cpu_info.cpu_percent = current_process.cpu_percent()
            process_cpu_info.create_time = datetime.fromtimestamp(current_process.create_time(),tzlocal()).strftime("%Y-%m-%d %H:%M:%S")
            process_cpu_info.ppid = current_process.ppid()
            process_cpu_info.status = current_process.status()
            if current_process.parent()!= None:
                process_cpu_info.parent_pid = current_process.parent().pid
                process_cpu_info.parent_name = current_process.parent().name()
            process_cpu_info.username = current_process.username()
            process_cpu_info.cwd = current_process.cwd()
            process_cpu_info.executable = current_process.exe()
            process_cpu_info.commandline = current_process.cmdline()
            process_cpu_info.host = socket.gethostname()
            process_cpu_info.event_time =  datetime.now(tzlocal())
            index_name = 'current_process_cpu_info_stats'
            if self.index_names != None:
                if 'CurrentProcessCpuInfoStats' in self.index_names:
                    index_name = self.index_names['CurrentProcessCpuInfoStats']
            process_cpu_info.meta.index = self.format_index(index_name)
            if self.logger != None:
                self.logger.info("%s_%s"%("CURRENT",str(process_cpu_info)))
            if self.save_to_elasticsearch:
                process_cpu_info.save()
            print('Saved Current Process CPU Info Stats using index : %s' % index_name)

    def save_current_process_memory_info(self):
        current_process = psutil.Process()
        meminfo =  current_process.memory_info()
        process_memory_info = ProcessMemoryInfoStats()
        try:
            process_memory_info.pid = current_process.pid
            process_memory_info.rss = meminfo.rss
            process_memory_info.vms = meminfo.vms
            process_memory_info.shared = meminfo.shared
            process_memory_info.text = meminfo.text
            process_memory_info.lib = meminfo.lib
            process_memory_info.data = meminfo.data
            process_memory_info.dirty = meminfo.dirty
            process_memory_info.uss = meminfo.uss
            process_memory_info.pss = meminfo.pss
            process_memory_info.swap = meminfo.swap
        except Exception as e:
            pass
        process_memory_info.host = socket.gethostname()
        process_memory_info.event_time =  datetime.now(tzlocal())
        index_name = 'current_process_memory_info_stats'
        if self.index_names != None:
            if 'CurrentProcessMemoryInfoStats' in self.index_names:
                index_name = self.index_names['CurrentProcessMemoryInfoStats']
        process_memory_info.meta.index = self.format_index(index_name)
        if self.logger != None:
            self.logger.info("%s_%s" % ("CURRENT",str(process_memory_info)))
        if self.save_to_elasticsearch:
            process_memory_info.save()
        print('Saved Current Process Memory Info Stats using index : %s' % index_name)

    def save_process_cpu_info(self,pid):
        current_process = psutil.Process(pid=pid)
        with current_process.oneshot():
            process_cpu_info = ProcessCpuInfoStats()
            process_cpu_info.pid = pid
            process_cpu_info.name = current_process.name()
            process_cpu_info.cpu_times = current_process.cpu_times()
            process_cpu_info.cpu_percent = current_process.cpu_percent()
            process_cpu_info.create_time = datetime.fromtimestamp(current_process.create_time(),tzlocal()).strftime("%Y-%m-%d %H:%M:%S")
            process_cpu_info.ppid = current_process.ppid()
            process_cpu_info.status = current_process.status()
            if current_process.parent()!= None:
                process_cpu_info.parent_pid = current_process.parent().pid
                process_cpu_info.parent_name = current_process.parent().name()
            process_cpu_info.username = current_process.username()
            process_cpu_info.cwd = current_process.cwd()
            process_cpu_info.executable = current_process.exe()
            process_cpu_info.commandline = current_process.cmdline()
            process_cpu_info.host = socket.gethostname()
            process_cpu_info.event_time =  datetime.now(tzlocal())
            index_name = 'process_cpu_info_stats'
            if self.index_names != None:
                if 'ProcessCpuInfoStats' in self.index_names:
                    index_name = self.index_names['ProcessCpuInfoStats']
            process_cpu_info.meta.index = self.format_index(index_name)
            if self.logger != None:
                self.logger.info("%s"%(str(process_cpu_info)))
            if self.save_to_elasticsearch:
                process_cpu_info.save()
            print('Saved Process %d CPU Info Stats using index : %s' % (pid,index_name))

    def save_process_memory_info(self,pid):
        current_process = psutil.Process(pid=pid)
        meminfo =  current_process.memory_info()
        try:
            process_memory_info = ProcessMemoryInfoStats()
            process_memory_info.pid = pid
            process_memory_info.rss = meminfo.rss
            process_memory_info.vms = meminfo.vms
            process_memory_info.shared = meminfo.shared
            process_memory_info.text = meminfo.text
            process_memory_info.lib = meminfo.lib
            process_memory_info.data = meminfo.data
            process_memory_info.dirty = meminfo.dirty
            process_memory_info.uss = meminfo.uss
            process_memory_info.pss = meminfo.pss
            process_memory_info.swap = meminfo.swap
        except Exception as e:
            pass
        process_memory_info.host = socket.gethostname()
        process_memory_info.event_time =  datetime.now(tzlocal())
        index_name = 'process_memory_info_stats'
        if self.index_names != None:
            if 'ProcessMemoryInfoStats' in self.index_names:
                index_name = self.index_names['ProcessMemoryInfoStats']
        process_memory_info.meta.index = self.format_index(index_name)
        if self.logger != None:
            self.logger.info("%s" % (str(process_memory_info)))
        if self.save_to_elasticsearch:
            process_memory_info.save()
        print('Saved Process Memory Info Stats using index : %s' % index_name)

    def save_process_connection_info(self,pid):
        current_process = psutil.Process(pid=pid)
        connections = current_process.connections()
        for connection in connections:
            process_connection_info = ProcessConnectionsStats()
            process_connection_info.pid = pid
            process_connection_info.fd = connection.fd
            try:
                process_connection_info.local_ip = connection.laddr[0]
            except Exception as e:
                process_connection_info.local_ip = ''
            
            try:
                process_connection_info.local_port = connection.laddr[1]
            except Exception as e:
                process_connection_info.local_port = -1

            try:
                process_connection_info.remote_ip = connection.raddr[0]
            except Exception as e:
                process_connection_info.remote_ip = ''
            
            try:
                process_connection_info.remote_port = connection.raddr[1]
            except Exception as e:
                process_connection_info.remote_port = -1
            
            process_connection_info.status = connection.status

            process_connection_info.host = socket.gethostname()
            process_connection_info.event_time =  datetime.now(tzlocal())
            index_name = 'process_connection_stats'
            if self.index_names != None:
                if 'ProcessConnectionsStats' in self.index_names:
                    index_name = self.index_names['ProcessConnectionsStats']
            process_connection_info.meta.index = self.format_index(index_name)
            if self.logger != None:
                self.logger.info(str(process_connection_info))
            if self.save_to_elasticsearch:
                process_connection_info.save()
            print('Saved Process %d Connection Stats using index : %s' % (pid,index_name))


