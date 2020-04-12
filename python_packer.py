#Python 3 tool to log data to MIDAS (feLabVIEW)
import sys
import threading
import zmq
import time
import socket
import struct
import datetime

import json

#Default behaviour is to use array as data type for logging...
import array
#Numpy is also supported
try:
    import numpy as np
    HaveNumpy=True
    print("Numpy found... np arrays are supported")
except:
    HaveNumpy=False
    print("Numpy not found... thats ok, but you can only use python arrays for data")


"""Timestamp functions"""
def GetLVTimeNow():
    #Get UNIX time now
    lvtime=datetime.datetime.utcnow().timestamp()
    #Add seconds between UTC 1/1/1904 and 1/1/1970
    lvtime+=2082844800.0
    #Convert to i64 seconds + u64 fraction of labview
    fraction=lvtime % 1
    seconds=int(lvtime-fraction)
    lvfraction=int(fraction*pow(2,64))
    #Pack timestamp into 128 bit struct
    LVTimestamp=struct.pack('qQ',seconds,lvfraction)
    return LVTimestamp

#This is hand coded... someone please check my calculation... check timezones?
def GetUnixTimeFromLVTime(timestamp):
    [UnixTime,Fraction]=struct.unpack('qQ',timestamp)
    UnixTime-=2082844800
    return UnixTime

"""Array type parsing functions"""
def GetArrayType(arg):
    switcher = {
        'd' : b"DBL\0" ,
        'f' : b"FLT\0" ,
        'l' : b"I32\0" ,
        'L' : b"U32\0" ,
    }
    return switcher.get(arg,"Unsupported array type ("+str(arg)+")... consider using floats?")

def GetNpArrayType(arg):
    switcher = {
        np.dtype('float64') : b"DBL\0" ,
        np.dtype('float32') : b"FLT\0" ,
        np.dtype('int32') : b"I32\0" ,
        np.dtype('uint32') : b"U32\0" ,
    }
    return switcher.get(arg,"Unsupported numpy array type ("+str(arg)+")... consider using floats?")

#I only support list of doubles!
def GetListType(arg):
    switcher = {
        type(float()) : b"DBL\0" ,
        #b"I64\0": int,
    }
    return switcher.get(arg,"Unsupported list type ("+str(arg)+")... consider using floats?")

"""Main DataPacker Object... use it as a global object, its thread safe"""
class DataPacker:
    """I have list of DataBanks"""
    RunNumber=-1
    RunStatus=str()
    PeriodicTasks=list()
    def __init__(self, experiment, flush_time=1):
        self.DataBanks=[]
        self.context = zmq.Context()
        #  Socket to talk to server
        print("Connecting to MIDAS server...")
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://"+experiment+":5555")
        print("Connection made...")
        print("Requesting to start logging")
        start_string=b"START_FRONTEND "+bytes(socket.gethostname(),'utf8')
        self.socket.send(start_string)
        response=self.socket.recv()
        get_addr=b"GIVE_ME_ADDRESS  "+bytes(socket.gethostname(),'utf8')
        self.socket.send(get_addr)
        address=self.socket.recv()
        print("Logging to address:"+address.decode("utf-8") )
        self.socket.disconnect("tcp://"+experiment+":5555")
        self.socket.connect(address)
        get_event_size=b"GIVE_ME_EVENT_SIZE"
        self.socket.send(get_event_size)
        EventSize=self.socket.recv()
        #Trim text before :, and remove all ' marks... ie get the integer out of the string
        self.MaxEventSize=int((str(EventSize).split(':')[1].replace('\'','')))
        print("MaxEventSize:"+str(self.MaxEventSize))
        t = threading.Thread(target=self.Run,args=(flush_time,))
        t.start()
        print("Polling thread launched")
    def AddData(self, catagory, varname, description, timestamp, data):
        #Matching bank not found in list... add this new bank to DataBanks list
        TYPE=b"NULL"
        #Convert any lists to an array
        if isinstance(data,list):
            TYPE=GetListType(type(data[0]))
            assert TYPE == b"DBL\0" , "list support is limited to doubles... please use arrays (or np arrays) for any other data type!"
            data=array.array('d',data)
        if HaveNumpy:
            #https://docs.scipy.org/doc/numpy/reference/arrays.dtypes.html
            if isinstance(data,np.ndarray):
                TYPE=GetNpArrayType(data.dtype)
                assert len(TYPE)==4 , str(TYPE)
                data=data.tobytes()
        #https://docs.python.org/3/library/array.html
        if isinstance(data,array.array):
            TYPE=GetArrayType(data.typecode)
            assert len(TYPE)==4 , str(TYPE)
            #Data need to be encoded as bytes... convert now
            data=data.tobytes()
        elif isinstance(data,str):
            data=bytearray(str(data), 'utf-8')
            TYPE=b"STR\0"
        elif isinstance(data,bytearray) or isinstance(data,bytes):
            if TYPE == b"NULL":
               TYPE=b"U8\0\0"
        else:
            print("Unsupported data format ("+str(type(data))+")... upgrade DataPacker!")
            exit(1)
        for bank in self.DataBanks:
            if bank.VARCATAGORY==catagory and bank.VARNAME==varname:
                #bank.print()
                bank.AddData(timestamp,data)
                return
        self.DataBanks.append(DataBank(TYPE,catagory,varname,description))
        self.AddData(catagory, varname, description, timestamp, data)
    def AddPeriodicTask(self,task):
        if task not in self.PeriodicTasks:
            self.PeriodicTasks.append(task)
    def GetRunNumber(self):
        #Launch the periodic task to track the RunNumber
        self.AddPeriodicTask("GET_RUNNO")
        #Wait until we have a valid RunNumber (happens on first call only)
        while self.RunNumber < 0:
           time.sleep(0.1)
        return self.RunNumber
    def GetRunStatus(self):
        #Launch the peridoc task to track Run Status
        self.AddPeriodicTask("GET_STATUS")
        while len(self.RunStatus) == 0:
            time.sleep(0.1)
        return self.RunStatus
    def UniqueToFlush(self):
        n=0
        for bank in self.DataBanks:
            if bank.NumberToFlush()>0:
                n+=1
        return n
    def NumberToFlush(self):
        n=0
        for bank in self.DataBanks:
            n+=bank.NumberToFlush()
        return n
    def Flush(self):
        #If data packer has no banks... do nothing
        if len(self.DataBanks)==0:
            return
        #If data packer has one bank, flush it
        if len(self.DataBanks)==1:
            return self.DataBanks[0].Flush()
        #If data packer only has one bank type to flush... flush it
        if self.UniqueToFlush()==1:
            for bank in self.DataBanks:
                if bank.NumberToFlush() > 0:
                   return bank.Flush()
        #If data packer has many banks to flush, put them in a superbank
        print("Building super bank")
        lump=b''
        number_of_banks=0
        for bank in self.DataBanks:
            n_to_flush=bank.NumberToFlush()
            if n_to_flush == 0:
                continue
            bank=bank.Flush()
            lump=struct.pack('{}s{}s'.format(len(lump),len(bank)),lump,bank)
            number_of_banks+=1
        super_bank=struct.pack('4s4sII{}s'.format(len(lump)),
                                        b"PYA1",
                                        b"PADD",
                                        len(lump),
                                        number_of_banks,
                                        lump)
        print("Size of lump in super bank:"+str(len(lump))+"("+str(number_of_banks)+" banks)")
        #You are about to send more data the MIDAS is ready to handle... throw an error! Increase the event_size in the ODB!
        assert(len(lump)<self.MaxEventSize)
        return super_bank
    def HandleReply(self, reply):
        ReplyList=json.loads(reply)
        RunnoList=[i for i, s in enumerate(ReplyList) if 'RunNumber:' in str(s)]
        for pos in RunnoList:
            self.RunNumber=int(str(ReplyList[pos]).split(':')[1].replace('\'',''))
        StatusList=[i for i, s in enumerate(ReplyList) if 'STATUS:' in str(s)]
        for pos in StatusList:
            self.RunStatus=str(ReplyList[pos]).split(':')[1].replace('\'','')
        HaveMsg=[i for i, s in enumerate(ReplyList) if 'Msg:' in str(s)]
        for msg in HaveMsg:
            print(ReplyList[msg])
        

    #Main (forever) loop for flushing the queues... run as its own thread
    def Run(self,sleep_time=1):
        #  Do 10 requests, waiting each time for a response
        while True:
            #Execute periodic tasks (RunNumber tracking etc)
            for task in self.PeriodicTasks:
                self.AddData(b"PERIODIC",bytes(task,'utf-8'),b"\0",GetLVTimeNow(),str("\0"))
            n=self.NumberToFlush()
            if n > 0:
                Bundle=self.Flush()
                print("Sending " +str(n) +" data bundles ("+str(len(Bundle)) +" bytes)...")
                self.socket.send(Bundle)
                print("Sent...")
                #  Get the reply.
                message = self.socket.recv()
                print("Received reply [ %s ]" % message)
                self.HandleReply(message)
                if message[0:5]==b"ERROR":
                    print("ERROR reported from MIDAS! FATAL!")
                    exit(1)
            else:
                print("Nothing to flush")
            time.sleep(sleep_time)
    
    #Early prototype for passing messages to MIDAS... 
    def AddMessage(self, message):
        if len(message)<30:
            #Short messages can use the 32 characters inside the header
            "MSGS" #message short
            #Messages more than 32 characters will have a character array
            "MSGL" #message long



class DataBank:
    LVBANK='4s4s16s16s32siiii{}s'
    LVDATA='16s{}s'
    r = threading.RLock()
    def __init__(self, datatype, catagory, varname,eqtype):
        self.BANK=b"PYB1"
        assert(isinstance(datatype,bytes))
        self.DATATYPE=datatype
        assert(isinstance(catagory,bytes))
        self.VARCATAGORY=catagory
        assert(isinstance(varname,bytes))
        self.VARNAME=varname
        assert(isinstance(eqtype,bytes))
        self.EQTYPE=eqtype
        self.DataList=[]
    def print(self):
        print("BANK:"+str(self.BANK))
        print("TYPE:"+str(self.DATATYPE))
        print("CATEGORY:"+str(self.VARCATAGORY))
        print("VARNAME:"+str(self.VARNAME))
        print("EQTYPE:"+str(self.EQTYPE))
        print("Datalist size:"+str(len(self.DataList)))
        if (len(self.DataList)):
           print("LVDATA size:"+str(len(self.DataList[0])))
    def AddData(self,timestamp,data):
        #print("Adding data to bank")
        lvdata=struct.pack(self.LVDATA.format(len(data)) ,timestamp,data)
        if len(self.DataList) > 0:
            assert len(self.DataList[0]) == len(lvdata)
        self.r.acquire()
        self.DataList.append(lvdata)
        self.r.release()
    def NumberToFlush(self):
        return len(self.DataList)
    def DataLengthOfAllBank(self):
        n=0
        for bank in self.DataList:
            n+=len(bank)
        return n
    def Flush(self):
        self.r.acquire()
        if len(self.DataList) == 0:
            print("Nothing in DataList to flush")
            self.r.release()
            return
        #print("Flushing")
        #print("Banks to flush:" + str(self.NumberToFlush() ) + " Data length:" + str(self.DataLengthOfAllBank()))
        lump=b''
        #Unfold data in DataList list
        for data in self.DataList:
           lump=struct.pack('{}s{}s'.format(len(lump),len(data)),lump,data)
        #Dimensions of LVDATA in BANK
        block_size=len(self.DataList[0])
        num_blocks=len(self.DataList)
        #self.print()
        self.DataList.clear()
        self.DataList=[]
        self.r.release()
        BANK=struct.pack(self.LVBANK.format(len(lump)),
                                        self.BANK,self.DATATYPE,
                                        self.VARCATAGORY,
                                        self.VARNAME,
                                        self.EQTYPE,
                                        1,2,
                                        block_size,num_blocks,
                                        lump)
        return BANK

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

ct_a=SimulateData(b"CatchingTrap",b"Array",b"PythonSimulation")
ct_np=SimulateData(b"CatchingTrap",b"NpArray",b"PythonSimulation")
ct_list=SimulateData(b"CatchingTrap",b"List",b"PythonSimulation")
at_p=SimulateData(b"AtomTrap",b"Pressure",b"PythonSimulation")
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
