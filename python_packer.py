
import sys
import threading
import zmq
#LVDATA='ll%s'
import time

import struct
import datetime
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

class DataPacker:
    """I have list of DataBanks"""
    TYPE="NULL"
    def AddData(self, catagory, varname, timestamp, data):
        for bank in self.DataBanks:
            if bank.VARCATAGORY==catagory and bank.VARNAME==varname:
                bank.AddData(timestamp,data)
                return
        #Matching bank not found in list... add this new bank to DataBanks list

        if type(data) is float:
            assert sys.float_info.mant_dig==53
            TYPE="DBLE"
        if type(data) is int:
            assert sys.int_info.bits_per_digit==30
            TYPE="INT3"
        self.DataBanks.append(DataBank(b"DBLE",catagory,varname,b"EquipmentType"))
        self.AddData(catagory, varname, timestamp, data)
    def __init__(self, experiment, flush_time=1):
        self.DataBanks=[]
        context = zmq.Context()
        #  Socket to talk to server
        print("Connecting to MIDAS server…")
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://localhost:5555")
        print("Connection made...")
        t = threading.Thread(target=self.Run,args=(flush_time,))
        t.start()
        print("Polling thread launched")
    def NumberToFlush(self):
        n=0
        for bank in self.DataBanks:
            n+=bank.NumberToFlush()
        return n
    def Flush(self):
        if len(self.DataBanks)==0:
            return
        if len(self.DataBanks)==1:
            return self.DataBanks[0].Flush()
        

    def Run(self,sleep_time=1):
        #  Do 10 requests, waiting each time for a response
        while True:
            n=self.NumberToFlush()
            if n > 0:
                Bundle=self.Flush()
                print("Sending " +str(n) +" data bundles...")
                #socket.send(b"Hello")
                #  Get the reply.
                #message = socket.recv()
                #print("Received reply [ %s ]" % message)
            else:
                print("Nothing to flush")
            time.sleep(sleep_time)
    def AddMessage(self, message):
        if len(message)<30:
            "MSGS" #message short
            "MSGL" #message long



class DataBank:
    LVBANK='4s4s16s16s32siiii{}s'
    LVDATA='8s{}d'
    DataList=[]
    r = threading.RLock()
    def __init__(self, datatype, catagory, varname,eqtype):
        self.BANK=b"PYB1"
        self.DATATYPE=datatype
        self.VARCATAGORY=catagory
        self.VARNAME=varname
        self.EQTYPE=eqtype
    def AddData(self,timestamp,data):
		
        print("Adding data to bank")
        print(self.LVDATA.format(len(data)))
        lvdata=struct.pack(self.LVDATA.format(len(data)) ,timestamp,*data)
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
        if len(self.DataList) == 0:
            print("Nothing in DataList to flush")
            return
        print("Flushing")
        print("Banks to flush:" + str(self.NumberToFlush() ) + " Data length:" + str(self.DataLengthOfAllBank()))
        #unfold data in DataList list
        self.r.acquire()
        block_size=len(self.DataList[0])
        num_blocks=len(self.DataList)
        lump=b''
        for data in self.DataList:
            lump=struct.pack('{}s{}s'.format(len(lump),len(data)),lump,data)
        self.DataList.clear()
        self.r.release()
        print("lump length:"+str(len(lump)))
        BANK=struct.pack(self.LVBANK.format(len(lump)),
                                        self.BANK,self.DATATYPE,
                                        self.VARCATAGORY,
                                        self.VARNAME,
                                        self.EQTYPE,
                                        1,0,
                                        block_size,num_blocks,
                                        lump)
        return BANK

class SimulateData:
    def __init__(self):
        self.packer=DataPacker("alphadaq")
    def Simulate(self):
        data=struct.pack('5d',0.1,0.2,0.3,0.4,0.5)
        self.packer.Flush()
        print("Adding data")
        self.packer.AddData(b"CatchingTrap",b"Temperature",GetLVTimeNow(),data)
        time.sleep(1)


s=SimulateData()
#t1 = threading.Thread(target=s.packer.Run(1))
#t2 = threading.Thread(target=s.Simulate())

s.Simulate()
s.Simulate()
s.packer.NumberToFlush()
#s.packer.Flush()



'''
uint64_t LVStyleUnixTime
uint64_t LVPrecision
char data[]
'''

'''
BANK(4x char)            DATATYPE(4x char)
VAR CATAGORY (16x char)

VAR NAME (16x char)

Equipment Type (32x char)



History Rate (uint32_t)   HASH?
Data Length (uint32_t)   Number of Entries (uint32_t)
LVDATA data[]
'''

'''
array=b"Elong array of stuffE"

lump=struct.pack(LVDATA.format(len(array)) ,1,2,array)
len(lump)
struct.unpack(LVDATA.format(len(array)),lump)

BANKNAME=b"BANK"
DATATYPE=b"CHAR"
VARCATAGORY=b"Catagory"
VARNAME=b"VariableName"
EQTYPE=b"Equipment Type"

BANK=struct.pack('4s4s16s16s32siiii{}s'.format(len(lump)),
                                        BANKNAME,DATATYPE,
                                        VARCATAGORY,
                                        VARNAME,
                                        EQTYPE,
                                        1,0,
                                        len(lump),1,
                                        lump)
'''
