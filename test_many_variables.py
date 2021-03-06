from MIDAS_GEM import *

#Global data packer
packer=DataPacker(midas_server="alphamidastest8",port=5555,max_data_rate=10000000) #10M



class SimulateData:
    def __init__(self,category,varname,description):
        self.category=category
        self.varname=varname
        self.description=description
        self.history_rate=1
    def GenerateArray(self, wait_time=1):
        #data=struct.pack('10d',0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)
        #print("Adding data")
        #packer.AnnounceOnSpeaker("THISHOST","LONG MESSAGE STRING THAT WILL TAKE SOME TIME TO READ OUT AND SOME MORE TIME")
        packer.AddData(self.category,self.varname,self.description,self.history_rate,GetLVTimeNow(),array.array('d',[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]))
        time.sleep(wait_time)
    def GenerateNpArray(self, wait_time=1):
        if HaveNumpy:
            packer.AddData(self.category,self.varname,self.description,self.history_rate,GetLVTimeNow(),np.array([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0],dtype = 'float64'))
        else:
            print("Please install numpy")
            exit(1)
        time.sleep(wait_time)
    def GenerateList(self,wait_time=1):
        packer.AddData(self.category,self.varname,self.description,self.history_rate,GetLVTimeNow(),[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0])
        time.sleep(wait_time)

print("Current Run Number: "+str(packer.GetRunNumber()))
print("Current Run Status: "+str(packer.GetRunStatus()))

BaseName=str(sys.argv[1])
assert len(BaseName)>0

DataGenerators=list()
#200 Variables of 10 doubles is approximately ALPHA 2
for i in range(10): #10 Categories
    for j in range(20): #20 Variables
        DataGenerators.append(SimulateData(BaseName+str(i),"Array"+str(j),"PythonSimulated spam"))
time.sleep(3)
while True:
    for i in range(10000):
        for j in DataGenerators:
            j.GenerateArray(0)
        time.sleep(0.1)
   #for i in range(10000):
   #   at_p.GenerateArray(1)
   
#ct_t.GenerateData(1)
#ct_t.GenerateData(1)
#ct_t.GenerateData(1)
