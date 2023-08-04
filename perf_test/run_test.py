#!/opt/B2B/anaconda3/bin/python
import sys
import argparse
import at.util
import logging
from at.qmanager import QueueManager
from datetime import datetime
import time
from at.dbutil import DBManager
from at.dbutil import Counter
from at.config import ConfigManager


def main():
    at.util.configDefaultLogger(20) # 10 debug, 20 info, 30 warn
    parser = argparse.ArgumentParser(description='SFTP Testing')
    parser.add_argument('-cf','--config-file', help='Config file path',required=True)
    parser.add_argument('-tc','--testcase',help='Name of testcase', required=True)
    parser.add_argument('-ti','--timeframe',help='How long to run in minutes', type=float,required=True)
    parser.add_argument('-ta','--tags',help='Tags if any', required=False)
    parser.add_argument('-rs','--reset-sequence',help='Reset Sequence', required=False)
    args = parser.parse_args()
    print('Received TAGS:%s'%(args.tags))
    logging.debug("starting queue manager")
    t1 = datetime.now()
    timeleft = int(args.timeframe * 60)
    elapsed = 0
    cfgmanager = ConfigManager(args.config_file)
    #dbfile = cfgmanager.get_dbfile(args.testcase)
    #dbmanager = DBManager(dbfile)
    dbmanager = DBManager()

    if args.reset_sequence != None:
        dbmanager.reset_db()
        print('Database sequence reset done ...')
        sys.exit(1)  
    #dbmanager.take_backup_and_clean()
    
    iteration = 1
    print('Process started')
    start_time = datetime.now()
    run_id = dbmanager.add_run_history_entry(args.testcase,args.timeframe,args.tags)
    print('Running ID:%d' % run_id)
    #while timeleft - elapsed > 0:
    print('Running iteration:%d' % iteration)
    ql = QueueManager(cfgmanager, dbmanager, args.testcase,start_time,timeleft)
    ql.start() # thread started
    ql.shutdown() # wait for all threads to finish
    time.sleep(5)
    t2 = datetime.now()
    delta = t2 - t1
    elapsed =  delta.seconds
    iteration = iteration + 1
    end_time = datetime.now()
    elapsed_time = end_time-start_time
    print('Delay of 60 seconds is introduced to complete the pending processes if any ...')
    time.sleep(60)
    print('Process completed!')
    dbmanager.update_run_history_entry(run_id,str(elapsed_time))
    #final_sequence = dbmanager.get_current_seqno()
    #dbmanager.update_sequence_counter(final_sequence)
    dbmanager.get_run_history_summary(run_id)
    dbmanager.close_connection()

if __name__ == '__main__':
    main()
