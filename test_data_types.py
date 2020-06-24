from MIDAS_GEM import *

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

ct_a=SimulateData("CatchingTrap","Array","PythonSimulation")
ct_np=SimulateData("CatchingTrap","NpArray","PythonSimulation")
ct_list=SimulateData("CatchingTrap","List","PythonSimulation")
at_p=SimulateData("AtomTrap","Pressure","PythonSimulation")
#time.sleep(1)
while True:
   for i in range(10000):
      ct_a.GenerateArray(1./3.)
      ct_np.GenerateNpArray(1./3.)
      ct_list.GenerateList(1./3.)

   #for i in range(10000):
   #   at_p.GenerateArray(1)
   
#ct_t.GenerateData(1)
#ct_t.GenerateData(1)
#ct_t.GenerateData(1)
