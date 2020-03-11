
import sys

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
    DataBanks=[]
    TYPE="NULL"
    def AddData(self, catagory, varname, timestamp, data):
        for bank in DataBanks:
            if bank.VARCATAGORY==catagory and bank.VARNAME==varname:
                bank.AddData(data)
                return
        #Matching bank not found in list... add this new bank to DataBanks list

        if type(data) is float:
            assert sys.float_info.mant_dig==53
            TYPE="DBLE"
        if type(data) is int:
            assert sys.int_info.bits_per_digit==30
            TYPE="INT3"
        DataBanks.append(DataBank("DBLE",catagory,varname,"EquipmentType"))
        self.AddData(catagory, varname, data)
    def __init__(self, experiment):
        context = zmq.Context()
        #  Socket to talk to server
        print("Connecting to MIDAS server…")
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://localhost:5555")
    def NumberToFlush(self):
        n=0
        for bank in DataBanks:
            n+=bank.NumberToFlush()
        return n
    def Flush(self):
        if len(DataBanks)==1:
            return DataBanks[0].Flush()

    def Run(self):
        #  Do 10 requests, waiting each time for a response
        for request in range(10):
            print("Sending request %s …" % request)
            socket.send(b"Hello")
            #  Get the reply.
            message = socket.recv()
            print("Received reply %s [ %s ]" % (request, message))



class DataBank:
    LVBANK='4s4s16s16s32siiii{}s'
    LVDATA='16b{}s'
    DataList=[]
    def __init__(self, datatype, catagory, varname,eqtype):
        self.BANK=b"PYB1"
        self.DATATYPE=datatype
        self.VARCATAGORY=catagory
        self.VARNAME=varname
        self.EQTYPE=eqtype
    def AddData(self,timestamp,data):
        lvdata=struct.pack(LVDATA.format(len(data)) ,timestamp,data)
        self.DataList.append(lvdata)
    def NumberToFlush(self):
        return len(DataList)
    def Flush(self):
        if len(DataList) == 0:
            return
        BANK=struct.pack('4s4s16s16s32siiii{}s'.format(len(lump)),
                                        self.BANK,self.DATATYPE,
                                        self.VARCATAGORY,
                                        self.VARNAME,
                                        EQTYPE,
                                        1,0,
                                        len(DataList[0]),len(DataList),
                                        DataList)


 

class SimulateData:
    packer=DataPacker("alphadaq")
    def Simulate():
        packer.AddData("CatchingTrap","Temperature",GetLVTimeNow(),[0.1 0.5 0.9])
        time.sleep(1)


s=SimulateData()
s.Simulate()
s.Simulate()
s.packer.NumberToFlush()
s.packer.Flush()



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
