from utils import ConfigManager
import argparse
import yaml
import time
from utils import OurLogger
import paramiko, threading
import time
from utils import AESCipher

class ssh:
    shell = None
    client = None
    transport = None
    runFlag = True
    last = ''
 
    def __init__(self, address, username, password):
        print("Connecting to server on ip", str(address) + ".")
        self.client = paramiko.client.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
        self.client.connect(address, username=username, password=password, look_for_keys=False)
        self.transport = paramiko.Transport((address, 22))
        self.transport.connect(username=username, password=password)
        print 'connected'
#        thread = threading.Thread(target=self.process)
#        self.thread.daemon = True
#        thread.start()
 
    def closeConnection(self):
        if(self.client != None):
            self.client.close()
            self.transport.close()
 
    def openShell(self):
        self.shell = self.client.invoke_shell()
 
    def sendShell(self, command):
        if(self.shell):
#            print 'sending command -> ' + command
            self.shell.send(command + "\n")
        else:
            print("Shell not opened.")
 
    def process(self):
        while True:
            # Print data when available
            if self.shell != None and self.shell.recv_ready():
                alldata = self.shell.recv(1024)
                while self.shell.recv_ready():
                    alldata += self.shell.recv(1024)
                strdata = str(alldata)
                strdata.replace('\r', '')
                print strdata
                if strdata == 'END': 
                    break
                self.last = strdata
                if(strdata.endswith("]") or strdata.endswith("$ ")):
                    break

def main():
    parser = argparse.ArgumentParser(description='Run Deploy Tool')
    parser.add_argument('-cf','--config-file', help='Config file path',required=True)
    parser.add_argument('-hg','--hosts-group', help='Hosts Group',required=False)
    parser.add_argument('-hl','--hosts-list', help='Comma separated hosts list',required=False)
    parser.add_argument('-dg','--deployment-groups', help='Comma separated deployment groups',required=True)

    args = parser.parse_args()
    config = None
    hosts_list = None
    hosts_group = None
    deployment_groups = None
    if args.config_file:
        config = ConfigManager(args.config_file)
        hosts_list = args.hosts_list
        hosts_group = args.hosts_group
        deployment_groups = args.deployment_groups
        #hosts = config.get_value('hosts')
        hosts = None
        if hosts_group != None:
            hosts = config.get_value(hosts_group)
        else:
            hosts = hosts_list.split(',')
        
        if hosts != None:
            deployments = deployment_groups.split(',')
            for host in hosts:
                host_info = config.get_value(host)
                if host_info == None:
                    print("")
                    print('Invalid HOST %s' % host)
                    print("================================")
                    continue

                print('Deploying To : Host Name:%s, Port:%s, Username:%s' % (host_info['host'],host_info['port'],host_info['username']))
                secureobj = AESCipher(host_info['secretkey'])
                sshPassword = secureobj.decrypt(host_info['password']) 
                print(sshPassword)
                connection = ssh(host_info['host'], host_info['username'], sshPassword)
                connection.openShell()   
                for deployment  in deployments:
                    #functions = config.get_value('deploy')
                    functions = config.get_value(deployment)
                    if functions == None:
                        print("")
                        print("Invalid function group:%s" % functions)
                        print("========================================")
                        continue
                    for function in functions:
                        print('Running Function:%s' % function)
                        commands = config.get_value(function)
                        if commands == None:
                            print("")
                            print("Invalid commands group:%s" % commands)
                            print("=====================================")
                            continue
                        for command in commands:
                            print('Running command %s'% command)
                            connection.sendShell(command)
                            connection.process()

        else:
            print("")
            print("Invalid host details provided!")
            print("=============================")

if __name__ == "__main__":
    main()
