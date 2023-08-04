from at.qmanager import QueueHandler
import pysftp
import logging
import traceback

class sftp_handler(QueueHandler):

    def __init__(self,known_hosts_file):
        logging.info('SFTP Handler initialised')
        try:
            self.cnopts = pysftp.CnOpts(knownhosts=known_hosts_file)
            # If the hostkey is present, it will avoid running it interactively
            # first time before we starting using the script
            self.cnopts.hostkeys = None
        except Exception as e:
            logging.error('Error raised while getting cnopts from pysftp')
            logging.error(traceback.format_exc())

    def connect(self, cinfo):
        logging.info('Connecting to %s:%d' %(cinfo['host'],int(cinfo['port'])))
        self.cinfo = cinfo

    def login(self, uinfo):
        cinfo = self.cinfo
        if 'password' in uinfo:
            self.client = pysftp.Connection(host=cinfo['host'], port=int(cinfo['port']),  cnopts=self.cnopts, 
                                        username=uinfo['username'], password=uinfo['password'])
            logging.info('Login into server %s:%d using username %s '%(cinfo['host'],int(cinfo['port']),uinfo['username']))
        else:
            self.client = pysftp.Connection(host=cinfo['host'], port=int(cinfo['port']),  cnopts=self.cnopts,
                                        username=uinfo['username'], private_key=uinfo['private_key'])
            logging.info('Login into server %s:%d using username %s private key %s'%(cinfo['host'],int(cinfo['port']),uinfo['username'],uinfo['private_key']))

    def send(self, fname, remfile=None):
        chgdir = '/'
        if 'chgdir' in self.cinfo:
            chgdir = self.cinfo['chgdir']
        self.client.chdir(chgdir)
        if not remfile:
            logging.info('Sending file %s as %s' %(fname,fname))
            self.client.put(fname, fname,confirm=False)
        else:
            logging.info('Sending file %s as %s' %(fname,remfile))
            self.client.put(fname, remfile,confirm=False)


    def close(self):
        try:
            self.client.close()
        except:
            pass
