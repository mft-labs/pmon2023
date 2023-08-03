import logging
import logging.handlers
import yaml
import base64
from Crypto import Random
from Crypto.Cipher import AES
import hashlib
import json
from datetime import datetime
import calendar

class ConfigManager(object):
    def __init__(self,config_file):
        """
        Constructor that loads YAML file
        """
        self.config = yaml.load(file(config_file,'rb').read())

    def get_value(self,key):
        """
        Returns value if it exists
        """
        if key in self.config:
            return self.config[key]
        return None

    def get_listitem(self,key,index):
        if key in self.config:
            list = self.config[key]
            if index in list:
                return list[index]
        return None

    def get_dictitem(self,key,subkey):
        if key in self.config:
            obj = self.config[key]
            if subkey in obj:
                return obj[subkey]
        return None


class OurLogger(object):
    def __init__(self,location,logname,filename,logformat="%(asctime)s - %(name)s - %(levelname)s - %(message)s"):
        maxFileSize = 10 * 1024 * 1024
        self.location="%s/" % (location)
        self.filename = filename
        self.logname = logname
        self.logger = logging.getLogger(logname)
        self.logger.setLevel(logging.DEBUG)
        LOG_FILENAME = '%s/%s' % (self.location,self.filename)
        #handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=maxFileSize, backupCount=10)
        handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when="h",interval=1,backupCount=10)
        formatter = logging.Formatter(logformat)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def getLogger(self):
        return self.logger

class AESCipher(object):
    
    def __init__(self, key): 
        self.bs = 32
        self.key = hashlib.sha256(key.encode()).digest()

    def encrypt(self, raw):
        raw = self._pad(raw)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv + cipher.encrypt(raw))

    def decrypt(self, enc):
        enc = base64.b64decode(enc)
        iv = enc[:AES.block_size]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return self._unpad(cipher.decrypt(enc[AES.block_size:])).decode('utf-8')

    def _pad(self, s):
        return s + (self.bs - len(s) % self.bs) * chr(self.bs - len(s) % self.bs)

    @staticmethod
    def _unpad(s):
        return s[:-ord(s[len(s)-1:])]


