from MIDAS_GEM import *

# Global data packer, one create one of these
packer=DataPacker("alphamidastest8")

# You can get the RunNumber and RunStatus at any time.
# The first time these are called there is a small delay,
# after that every call is instantanious
print("Current Run Number: "+str(packer.GetRunNumber()))
print("Current Run Status: "+str(packer.GetRunStatus()))

while True:
    #Do some work...
    time.sleep(1)
    #Send results to MIDAS
    packer.AddData("CategoryName",
                   "VariableName",
                   "32 Character Description",
                   0,
                   1,
                   GetLVTimeNow(),
                   array.array('d',[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0])
                   )
    print("Current Run Number: "+str(packer.GetRunNumber()))
    #python arrays are prefered, numpy arrays are supported as well as lists of doubles
