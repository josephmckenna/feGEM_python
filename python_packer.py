
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
        self.context = zmq.Context()
        #  Socket to talk to server
        print("Connecting to MIDAS server…")
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://localhost:5555")
        print("Connection made...")
        t = threading.Thread(target=self.Run,args=(flush_time,))
        t.start()
        print("Polling thread launched")
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
        super_bank=struct.pack('4sII{}s'.format(len(lump)),
                                        b"PYA1",
                                        len(lump),
                                        number_of_banks,
                                        lump)
        print("Size of lump in super bank:"+str(len(lump))+"("+str(number_of_banks)+" banks)")
        return super_bank

    #Main (forever) loop for flushing the queues... run as its own thread
    def Run(self,sleep_time=1):
        #  Do 10 requests, waiting each time for a response
        while True:
            n=self.NumberToFlush()
            if n > 0:
                Bundle=self.Flush()
                print("Sending " +str(n) +" data bundles...")
                self.socket.send(Bundle)
                print("Sent...")
                #  Get the reply.
                message = self.socket.recv()
                print("Received reply [ %s ]" % message)
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
        #print("Adding data to bank")
        #print(self.LVDATA.format(len(data)))
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
        self.r.acquire()
        if len(self.DataList) == 0:
            print("Nothing in DataList to flush")
            self.r.release()
            return
        print("Flushing")
        print("Banks to flush:" + str(self.NumberToFlush() ) + " Data length:" + str(self.DataLengthOfAllBank()))
        #unfold data in DataList list
        block_size=len(self.DataList[0])
        num_blocks=len(self.DataList)
        lump=b''
        for data in self.DataList:
            lump=struct.pack('{}s{}s'.format(len(lump),len(data)),lump,data)
        self.DataList.clear()
        self.DataList=[]
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

#Global data packer
packer=DataPacker("alphadaq")

class SimulateData:
    def __init__(self,category,varname):
        self.category=category
        self.varname=varname
    def GenerateData(self, wait_time=1):
        data=struct.pack('5d',0.1,0.2,0.3,0.4,0.5)
        #print("Adding data")
        packer.AddData(self.category,self.varname,GetLVTimeNow(),data)
        time.sleep(wait_time)


ct_t=SimulateData(b"CatchingTrap",b"Temperature")
at_p=SimulateData(b"AtomTrap",b"Pressure")
for i in range(10):
   ct_t.GenerateData(0.0001)
   at_p.GenerateData(1)
ct_t.GenerateData(1)
ct_t.GenerateData(1)
ct_t.GenerateData(1)
