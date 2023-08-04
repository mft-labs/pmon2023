import random
import os
class FileManager(object):

    def __init__(self, path):
        self.path = path
        self.counter  = {}
        self.counter_max = {}
        self.filename = {}
        self.details  = {}
        self.processed = set([])
        self.details["5 K"] = 60.0
        self.details["100 K"] = 20.0
        self.details["250 K"] = 5.0
        self.details["500 K"] = 4.44
        self.details["1 Mb"] = 2.0
        self.details["5 Mb"] = 2.0
        self.details["10 Mb"] = 1.0
        self.details["50 Mb"] = 2.0
        self.details["100 Mb"] = 1.0
        self.details["500 Mb"] = 1.0
        self.details["1 Gb"] = 1.5
        self.details["5 Gb"] = 0.02
        self.details["10 Gb"] = 0.02
        self.details["15 Gb"] = 0.02

    def reset_counter(self, limit):
        total = 0
        for key in sorted(self.details.keys()):
            self.counter[key] = 0
            if self.details[key] * limit/100.0 > int(self.details[key] * limit/100.0) :
                self.counter_max[key] = int(self.details[key] * limit/100) + 1
                total += int(self.details[key] * limit/100.0) + 1
            else:
                self.counter_max[key] = int(self.details[key] * limit/100)
                total += int(self.details[key] * limit/100.0)
        #print('No. of files ', total )

    def next_file_to_choose(self):
        if len(self.processed) != len(self.details.keys()):
            choice = random.choice(sorted(list(set(self.details.keys())-self.processed)))
            self.counter[choice] += 1
            if self.counter[choice] == self.counter_max[choice]:
                self.processed.add(choice)
            return self.filename[choice]
        return None

    def generate_files(self):
        for key in sorted(self.details.keys()):
            #print(key, self.details[key], )
            arr = key.split(" ")
            if arr[1] == "K":
                size = 1024
            elif arr[1] == "Mb":
                size = 1024 * 1024
            elif arr[1] == "Gb":
                size = 1024 * 1024 * 1024
            self.filename[key] = self.path + os.path.sep + "file_{}_{}_{}".format(arr[0],arr[1],int(arr[0])*size)
            if os.path.exists(self.path+os.path.sep+"/file_{}_{}_{}".format(arr[0],arr[1],int(arr[0])*size)):
                pass
            else:
                f1 = open(self.path + os.path.sep + "file_{}_{}_{}".format(arr[0],arr[1],int(arr[0])*size), "wb")
                f1.truncate(int(arr[0])*size)
                f1.close()

           
if __name__ == "__main__":
    fm = FileManager("dump")
    fm.generate_files()
    fm.reset_counter(5000)
    count = 0 
    #fw = open("result.txt","w")
    #fw.close()
    while True:
        choice = fm.next_file_to_choose()
        if choice!=None:
            #fw = open("result.txt","a")
            #fw.write(choice+"\n")
            #fw.close()
            #print(choice)
            count += 1
        else:
            break
    print('No. of files processed: ',count)


    





    