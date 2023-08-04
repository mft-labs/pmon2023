#Config Manager
from configparser import ConfigParser
import sys

class ConfigManager(object):
    def __init__(self,config_file):
        self.config_file = config_file
        self.config = ConfigParser()
        self.config.readfp(open(config_file))

    def get_section(self,section):
        return dict(self.config.items(section))

    def get_element(self,section,element):
        params = self.get_section(section)
        return params[element.strip()].strip()

    def get_sfg_hosts(self,section):
        sfg_hosts =  []
        hlist = self.get_element(section,'sfg-hosts').replace(' ', '').split(",")
        for host in hlist:
            cinfo = self.get_section(host.strip())
            sfg_hosts.append(cinfo)
        return sfg_hosts

    def get_sfg_users(self,section):
        sfg_users = []
        ulist = self.get_element(section,'sfg-users').replace(' ', '').split(",")
        for user in ulist:
            uinfo = self.get_section(user.strip())
            sfg_users.append(uinfo)
        return sfg_users

    def get_dbfile(self,section):
        return self.get_element(section,'db-file')

    def get_knownhosts_file(self,section):
        return self.get_element(section,'known-hosts-file')

    def get_file_format(self,section):
        return self.get_element(section,'file-format')

    
if __name__ == '__main__':
    config_file = sys.argv[1]
    section = sys.argv[2]
    cfgmanager = ConfigManager(config_file)
    #print('Thread Count: %s' % cfgmanager.get_element(section,'thread-count'))
    #print('Known Hosts File: %s' % cfgmanager.get_element(section,'known-hosts-file'))
    #print('DB File: %s' % cfgmanager.get_dbfile(section))
    print('SFT Hosts: %s' % cfgmanager.get_sfg_hosts(section))
    print('SFG Users: %s' % cfgmanager.get_sfg_users(section))
