#Python 3 tool to log data to MIDAS (feLabVIEW)

#Standard libraries:
import sys
import threading
import time
import socket
import struct
import datetime
import json
import array #Default behaviour is to use array as data type for logging...

#External libraries:
import zmq
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

def CleanString(arg,length):
    if isinstance(arg,str):
        arg=bytes(arg[0:length],'utf8')
    return arg

"""Main DataPacker Object... use it as a global object, its thread safe"""
class DataPacker:
    """I have list of DataBanks"""
    RunNumber=-1
    RunStatus=str()
    PeriodicTasks=list()
    
    def __init__(self, experiment, flush_time=1):
        self.DataBanks=[]
        self.context = zmq.Context()

        #  Connect to LabVIEW frontend 'supervisor'
        print("Connecting to MIDAS server...")
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://"+experiment+":5555")
        print("Connection made... Requesting to start logging")
        start_string=b"START_FRONTEND "+bytes(socket.gethostname(),'utf8')
        self.socket.send(start_string)
        response=self.socket.recv()
        get_addr=b"GIVE_ME_ADDRESS "+bytes(socket.gethostname(),'utf8')
        self.socket.send(get_addr)
        address=self.socket.recv()
        print("Logging to address:"+address.decode("utf-8") )
        self.socket.disconnect("tcp://"+experiment+":5555")

        # Connect to LabVIEW frontend 'worker' (where we send data)
        self.socket.connect(address)
        # Request the max data pack size
        get_event_size=b"GIVE_ME_EVENT_SIZE"
        self.socket.send(get_event_size)
        self.MaxEventSize=-1
        self.__HandleReply(self.socket.recv())
        print("MaxEventSize:"+str(self.MaxEventSize))

        # Stack background thread to flush data
        t = threading.Thread(target=self.__Run,args=(flush_time,))
        t.start()
        print("Polling thread launched")

    def AnnounceOnSpeaker(self,message):
        self.AddData(b"PYSYSMON",b"TALK",b"\0",GetLVTimeNow(),message)

    def AddData(self, catagory, varname, description, timestamp, data):
        #Clean up input strings... (convert str to bytes and trim length)
        catagory=CleanString(catagory,16)
        varname=CleanString(varname,16)
        description=CleanString(description,32)
        #Default data type
        TYPE=b"NULL"

        #Convert any lists to an array
        if isinstance(data,list):
            TYPE=GetListType(type(data[0]))
            assert TYPE == b"DBL\0" , "list support is limited to doubles... please use arrays (or np arrays) for any other data type!"
            data=array.array('d',data)
        if HaveNumpy:
            #https://docs.scipy.org/doc/numpy/reference/arrays.dtypes.html
            if isinstance(data,np.ndarray): #Convert numpy array to byte array
                TYPE=GetNpArrayType(data.dtype)
                assert len(TYPE)==4 , str(TYPE)
                data=data.tobytes()
        #https://docs.python.org/3/library/array.html
        if isinstance(data,array.array): #Convert python array to byte array
            TYPE=GetArrayType(data.typecode)
            assert len(TYPE)==4 , str(TYPE)
            #Data need to be encoded as bytes... convert now
            data=data.tobytes()
        elif isinstance(data,str): #Convert string data to byte array
            data=bytearray(str(data), 'utf-8')
            TYPE=b"STR\0"
        #Unknown data type... maybe the user is logging a 'blob' of data
        elif isinstance(data,bytearray) or isinstance(data,bytes): 
            if TYPE == b"NULL":
               TYPE=b"U8\0\0"
        else:
            print("Unsupported data format ("+str(type(data))+")... upgrade DataPacker!")
            exit(1)

        #Find existing bank to add data to
        for bank in self.DataBanks:
            if bank.VARCATAGORY==catagory and bank.VARNAME==varname:
                #Bank already in memory! Add data to it!
                bank.AddData(timestamp,data)
                return
        #Matching bank not found in list... add this new bank to DataBanks list
        self.DataBanks.append(DataBank(TYPE,catagory,varname,description))
        self.AddData(catagory, varname, description, timestamp, data)

    #Add a task that is called once per second (eg track RunNumber).(Is private function)
    def __AddPeriodicRequestTask(self,task):
        if task not in self.PeriodicTasks:
            self.PeriodicTasks.append(task)

    def GetRunNumber(self):
        #Launch the periodic task to track the RunNumber
        self.__AddPeriodicRequestTask("GET_RUNNO")
        #Wait until we have a valid RunNumber (happens on first call only)
        while self.RunNumber < 0:
           time.sleep(0.1)
        return self.RunNumber

    def GetRunStatus(self):
        #Launch the peridoc task to track Run Status
        self.__AddPeriodicRequestTask("GET_STATUS")
        while len(self.RunStatus) == 0:
            time.sleep(0.1)
        return self.RunStatus

    #Check all banks for data that needs flushing
    def __BanksToFlush(self):
        n=0
        for bank in self.DataBanks:
            if bank.NumberToFlush()>0:
                n+=1
        return n

    #Flatten all data in memory (to send to MIDAS)
    def __Flush(self):
        #If data packer has no banks... do nothing
        if len(self.DataBanks)==0:
            return
        #If data packer has one bank, flush it
        if len(self.DataBanks)==1:
            return self.DataBanks[0].Flush()
        #If data packer only has one bank type to flush... flush it
        if self.__BanksToFlush()==1:
            for bank in self.DataBanks:
                if bank.NumberToFlush() > 0:
                    assert(bank.DataLengthOfAllBank()+88<self.MaxEventSize)
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

    #Check for the string 'item' in json_list, update target if found
    def __ParseReplyItem(self,json_list,item,target):
        ItemList=[i for i, s in enumerate(json_list) if item in str(s)]
        print(json_list)
        target_type=type(target)
        print(item)
        for i in ItemList:
            target=target_type(str(json_list[i]).split(':')[1].replace('\'',''))
            print("TARGET:")
            print(target)

    #Print all 'item' in json_list
    def __PrintReplyItems(self,json_list,item):
        HaveMsg=[i for i, s in enumerate(json_list) if item in str(s)]
        for msg in HaveMsg:
            print(ReplyList[msg])

    #Parse the json string MIDAS sends as a reply to data
    def __HandleReply(self, reply):
        #Unfold the json string into a list
        ReplyList=json.loads(reply)
        self.__ParseReplyItem(ReplyList,'RunNumber:',self.RunNumber)
        self.__ParseReplyItem(ReplyList,'EventSize:',self.MaxEventSize)
        self.__ParseReplyItem(ReplyList,'STATUS:',self.RunStatus)
        self.__PrintReplyItems(ReplyList,'Msg:')
        self.__PrintReplyItems(ReplyList,'Err:')

    #Main (forever) loop for flushing the queues... run as its own thread
    def __Run(self,sleep_time=1):
        #Announce I am connection on MIDAS speaker
        connectMsg="New python connection from "+str(socket.gethostname()) + " PROGRAM:"+str(sys.argv)
        print(connectMsg)
        self.AnnounceOnSpeaker(connectMsg)
        
        # Run forever!
        while True:
            # Execute periodic tasks (RunNumber tracking etc)
            for task in self.PeriodicTasks:
                self.AddData(b"PERIODIC",bytes(task,'utf-8'),b"\0",GetLVTimeNow(),str("\0"))
            # Flatten data in memory and send to MIDAS (if there is any data)
            n=self.__BanksToFlush()
            if n > 0:
                Bundle=self.__Flush()
                print("Sending " +str(n) +" banks of data ("+str(len(Bundle)) +" bytes)...")
                self.socket.send(Bundle)
                print("Sent...")
                #  Get the reply.
                message = self.socket.recv()
                print("Received reply [ %s ]" % message)
                self.__HandleReply(message)
                if message[0:5]==b"ERROR":
                    print("ERROR reported from MIDAS! FATAL!")
                    exit(1)
            else:
                print("Nothing to flush")
            time.sleep(sleep_time)

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
