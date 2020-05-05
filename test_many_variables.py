from python_packer import *

#Global data packer
packer=DataPacker("alphamidastest8")



class SimulateData:
    def __init__(self,category,varname,description):
        self.category=category
        self.varname=varname
        self.description=description
    def GenerateArray(self, wait_time=1):
        #data=struct.pack('10d',0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)
        #print("Adding data")
        packer.AddData(self.category,self.varname,self.description,GetLVTimeNow(),array.array('d',[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]))
        time.sleep(wait_time)
    def GenerateNpArray(self, wait_time=1):
        if HaveNumpy:
            packer.AddData(self.category,self.varname,self.description,GetLVTimeNow(),np.array([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0],dtype = 'float64'))
        else:
            print("Please install numpy")
            exit(1)
        time.sleep(wait_time)
    def GenerateList(self,wait_time=1):
        packer.AddData(self.category,self.varname,self.description,GetLVTimeNow(),[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0])
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
time.sleep(10)
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
