from monitor.utils import AESCipher
import getpass




if __name__ == "__main__":
    print("Password Encryption Utility")
    secretkey = raw_input("Enter 16 or 24 or 32 characters code: ")
    secword = getpass.getpass(prompt='Enter Password: ', stream=None)
    #secure = OurCrypto(secretkey)
    secure = AESCipher(secretkey)
    print("Encrypted Password is :%s" % (secure.encrypt(secword)))

