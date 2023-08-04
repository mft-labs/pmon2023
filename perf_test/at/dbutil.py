import sqlite3
import psycopg2
from datetime import datetime
import logging
import time
import os
from multiprocessing import RawValue, Lock
thread_safe_counter = None


class Counter(object):
    def __init__(self, value=0):
        # RawValue because we don't need it to create a Lock:
        self.val = RawValue('i', value)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value



class DBManager(object):
    def __init__(self):
        global thread_safe_counter
        #self.db_file = db_file
        self.conn = None
        #self.counter = Counter(0)
        #if not os.path.exists(db_file):
        #    self.setup_db()
        conn = self.get_connection()
        current_sequence = self.get_current_sequence_counter()
        if thread_safe_counter == None:
            thread_safe_counter = Counter(current_sequence)
        self.lock = Lock()

    def get_connection(self):
        try :
            #return sqlite3.connect(self.db_file)
            return psycopg2.connect(host="10.31.1.169", database="perf", user="eb2bgate", password="GFriXGXOXxj3GJKjpDGp")
        except:
            return None



    def setup_db(self):
        conn = self.get_connection()
        if conn != None:
            cursor = conn.cursor()
            cursor.execute("""CREATE TABLE file_sequence (sequence_number INTEGER DEFAULT (1) );""")
            cursor.execute("""CREATE TABLE fileactivity (
                                seqno      INTEGER PRIMARY KEY AUTOINCREMENT
                                                NOT NULL,
                                filename   TEXT,
                                remotefile TEXT,
                                filesize   INT,
                                details    TEXT,
                                status     TEXT,
                                start_time TEXT,
                                end_time   TEXT,
                                time_taken TEXT,
                                run_id     INTEGER REFERENCES run_history (run_id),
                                host       TEXT,
                                port       TEXT,
                                username   TEXT
                            );""")
            cursor.execute("""CREATE TABLE run_history (
                                run_id             INTEGER PRIMARY KEY AUTOINCREMENT
                                                        NOT NULL
                                                        DEFAULT (1),
                                test_case          TEXT,
                                start_time         TEXT,
                                end_time           TEXT,
                                time_frame         TEXT,
                                no_of_transactions INTEGER,
                                elapsed_time       TEXT,
                                num_success        INTEGER,
                                num_failed         INTEGER,
                                tags               TEXT
                            );""")
            conn.commit()
            conn.close()

    def get_num_success(self):
        conn = self.get_connection()
        if conn!= None:
            cursor = conn.cursor()
            cursor.execute("select count(status) from fileactivity where status='SUCCESS' and run_id=%d" %(self.run_id))
            row = cursor.fetchone()
            conn.close()
            return int(row[0])
        return None

    def get_num_failed(self):
        conn = self.get_connection()
        if conn != None:
            cursor = conn.cursor()
            cursor.execute("select count(status) from fileactivity where status='FAILED' and run_id=%d" %(self.run_id))
            row = cursor.fetchone()
            conn.close()
            return int(row[0])
        return None


    def add_info(self, seqno, filename, remotefile, filesize, details,status,start_time,end_time,time_taken,host,port,username):
        conn = self.get_connection()
        if conn != None:
            cursor = conn.cursor()
            filesize = int(filesize)
            try:
                run_id = self.run_id
                logging.info("""insert into fileactivity(filename,filesize,remotefile,details,status,start_time,end_time,time_taken,run_id,host,port,username) values('%s',%d,'%s',%s','%s','%s','%s','%s',%d,'%s','%s','%s')""" %(filename, filesize, remotefile, details,status,start_time,end_time,time_taken,run_id,host,port,username))
                cursor.execute("""insert into fileactivity(filename,filesize,remotefile,details,status,start_time,end_time,time_taken,run_id,host,port,username) values('%s',%d,'%s','%s','%s','%s','%s','%s',%d,'%s','%s','%s')""" %(filename, filesize, remotefile, details,status,start_time,end_time,time_taken,run_id,host,port,username))
                conn.commit()
            except Exception as e:
                logging.error('Exception raised in add_info %s' % str(e))
            finally:
                try:
                    conn.close()
                except:
                    pass
                
            #conn.commit()

    def take_backup_and_clean(self):
        conn = self.get_connection()
        if conn != None:
            cursor = conn.cursor()
            #cursor.execute("""create table fileactivity_%s as select * from fileactivity""" %(str(time.time()).replace('.','_')))
            cursor.execute("""create table fileactivity_%d as select * from fileactivity""" %(self.get_current_run_id()))
            conn.commit()
            cursor = conn.cursor()
            cursor.execute("""delete from fileactivity""")
            conn.commit()
            conn.close()

    def add_run_history_entry(self,testcase,timeframe,tags):
        conn = self.get_connection()
        if conn != None:
            cursor = conn.cursor()
            cursor.execute("""insert into run_history(test_case,time_frame,start_time,tags) values('%s','%s','%s','%s') returning run_id""" %(testcase,timeframe,str(datetime.now()),tags))
            run_id = cursor.fetchone()[0]
            conn.commit()
            self.run_id = run_id
            #print("Returning run_id %d" % self.run_id)
            return self.run_id
            #cusor = conn.cursor()
            #cursor.execute("""SELECT max(run_id) FROM run_history""")
            #row = cursor.fetchone()
            #conn.commit()
            #conn.close()
            #return int(row[0])
        return None

    def reset_db(self):
        conn = self.get_connection()
        if conn!=None:
            cursor = conn.cursor()
            try:
                cursor.execute("drop table fileactivity_0")
            except:
                pass
            cursor.execute("delete from file_sequence")
            cursor.execute("delete from run_history")
            cursor.execute("delete from sqlite_sequence where name='run_history'")
            cursor.execute("delete from sqlite_sequence where name='file_sequence'")
            conn.commit()
            conn.close()
            return True
        return False

    def get_transaction_count(self):
        conn = self.get_connection()
        if conn != None:
            cursor = conn.cursor()
            cursor.execute("""select count(seqno) from fileactivity where run_id=%d""" %(self.run_id))
            row = cursor.fetchone()
            conn.commit()
            conn.close()
            return int(row[0])
        return None

    def update_run_history_entry(self,run_id,elapsed_time):
        conn = self.get_connection()
        if conn != None:
            cursor = conn.cursor()
            num_transactions = self.get_transaction_count()
            cursor.execute("""update run_history set no_of_transactions='%s', end_time='%s', elapsed_time='%s', num_success=%d, num_failed=%d where run_id=%d""" % (num_transactions,str(datetime.now()),elapsed_time,self.get_num_success(),self.get_num_failed(),self.run_id))
            conn.commit()
            conn.close()
            return True
        return False

    def get_current_run_id(self):
        conn = self.get_connection()
        if conn != None:
            cursor = conn.cursor()
            cursor.execute("""SELECT max(run_id) FROM run_history""")
            row = cursor.fetchone()
            conn.commit()
            conn.close()
            if row[0] == None:
                return 0
            return int(row[0])
        return None

    def get_current_seqno(self):
        with self.lock:
            global thread_safe_counter
            return thread_safe_counter.value()

    def get_next_seqno(self):
        with self.lock:
            global thread_safe_counter
            thread_safe_counter.increment()
            return thread_safe_counter.value()

    def update_sequence_counter(self,newvalue):
        conn = self.get_connection()
        if conn!=None:
            cursor = conn.cursor()
            cursor.execute("update file_sequence set sequence_number=%d" % newvalue)
            conn.commit()
            conn.close()

    def get_current_sequence_counter(self):
        conn = self.get_connection()
        if conn!=None:
            cursor = conn.cursor()
            cursor.execute("select sequence_number from file_sequence LIMIT 1")
            row = cursor.fetchone()
            if (row == None) or (row[0] == None):
                cursor.execute("insert into file_sequence(sequence_number) values(0)")
                conn.commit()
                conn.close()
                return 0
            conn.close()
            return int(row[0])
        return 0

    def get_run_history_summary(self,run_id):
        #print("Using run_id %d " % self.run_id)
        conn = self.get_connection()
        if conn != None:
            cursor = conn.cursor()
            cursor.execute("""select run_id, test_case, start_time, end_time,time_frame,no_of_transactions,elapsed_time,num_success, num_failed from run_history where run_id = %d""" % self.run_id)
            row = cursor.fetchone()
            print("Summary of Test Run\n")
            print("=============================================================================\n")
            print("Run Id: %s" % row[0])
            print("Test Case: %s" % row[1])
            print("Start Time: %s" % row[2])
            print("End Time: %s" % row[3])
            print("Time Frame: %s" % row[4] )
            print("No. of Transactions: %s" % row[5])
            print("Elapsed Time: %s" % row[6])
            print("No. of successful transactions: %s" % row[7])
            print("No. of failed transactions: %s" % row[8])
            print("=============================================================================\n")
            cursor.close()
            conn.commit()
            conn.close()
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("""select host,port,username,status,count(*) from fileactivity group by run_id,host,port,username,status having run_id=%d order by host,port,username,status""" % self.run_id)
            rows = cursor.fetchall()
            print('{:35s} {:6s} {:20s} {:10s} {:6s}'.format('Hostname','Port','Username','Status','Count'))
            print("=============================================================================\n")
            for row in rows:
                print('{:35s} {:6s} {:20s} {:10s} {:6d}'.format(row[0],row[1],row[2],row[3],row[4]))
            print("=============================================================================\n")
            cursor.close()
            conn.commit()
            conn.close()


    def close_connection(self):
        try:
            self.conn.close()
        except:
            pass
        self.conn = None
