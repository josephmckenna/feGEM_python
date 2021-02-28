#!python3
from MIDAS_GEM import *

#Global data packer
packer=DataPacker(midas_server="thevoid",port=12345,max_data_rate=100000)

#packer.TurnOnTestMode()
#packer.TurnOnDebugMode()
#packer.TurnOffDebugMode()

class SimulateData:
    def __init__(self,category,varname,description):
        self.category=category
        self.varname=varname
        self.description=description
        self.history_settings=0
        self.history_rate=1
    def GenerateArray(self, wait_time=1):
        #data=struct.pack('10d',0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)
        #print("Adding data")
        #packer.AnnounceOnSpeaker("THISHOST","LONG MESSAGE STRING THAT WILL TAKE SOME TIME TO READ OUT AND SOME MORE TIME")
        packer.AddData(self.category,self.varname,self.description,self.history_settings,self.history_rate,GetLVTimeNow(),array.array('d',[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]))
        time.sleep(wait_time)
    def GenerateNpArray(self, wait_time=1):
        if HaveNumpy:
            packer.AddData(self.category,self.varname,self.description,self.history_settings,self.history_rate,GetLVTimeNow(),np.array([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0],dtype = 'float64'))
        else:
            print("Please install numpy")
            exit(1)
        time.sleep(wait_time)
    def GenerateList(self,wait_time=1):
        packer.AddData(self.category,self.varname,self.description,self.history_settings,self.history_rate,GetLVTimeNow(),[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0])
        time.sleep(wait_time)

print("Current Run Number: "+str(packer.GetRunNumber()))
print("Current Run Status: "+str(packer.GetRunStatus()))

BaseName=str(sys.argv[1])
assert len(BaseName)>0

DataGenerators=list()
#200 Variables of 10 doubles is approximately ALPHA 2
#Here I only create 10
for i in range(2): #2 Categories
    for j in range(5): #5 Variables
        DataGenerators.append(SimulateData(BaseName+str(i),"Array"+str(j),"PythonSimulated spam"))
#time.sleep(1)
while True:
    for i in range(10000):
        for j in DataGenerators:
            j.GenerateArray(0)
        time.sleep(1)
   #for i in range(10000):
   #   at_p.GenerateArray(1)
   
#ct_t.GenerateData(1)
#ct_t.GenerateData(1)
#ct_t.GenerateData(1)
