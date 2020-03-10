import struct
#LVDATA='ll%s'
import sys

import time
import zmq





class DataPacker:
    """I have list of DataBanks"""
    DataBanks=[]
    TYPE="NULL"
    def AddData(self, catagory, varname, data):
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

    def Run():
        #  Do 10 requests, waiting each time for a response
        for request in range(10):
            print("Sending request %s …" % request)
            socket.send(b"Hello")
            #  Get the reply.
            message = socket.recv()
            print("Received reply %s [ %s ]" % (request, message))



class DataBank:
    LVBANK='4s4s16s16s32siiii{}s'
    def __init__(self, datatype, catagory, varname,eqtype):
        self.BANK=b"LVB1"
        self.DATATYPE=datatype
        self.VARCATAGORY=catagory
        self.VARNAME=varname
        self.EQTYPE=eqtype
    def AddData(self,data):
        self.data=data
 

LVDATA='QQ{}s'
'''
uint64_t NTS
uint64_t LVTS
char data[]
'''
LVBANK='4s4s16s16s32siiii{}b'
'''
BANK(4x char)            DATATYPE(4x char)
VAR CATAGORY (16x char)

VAR NAME (16x char)

Equipment Type (32x char)



History Rate (uint32_t)   HASH?
Data Length (uint32_t)   Number of Entries (uint32_t)
LVDATA data[]
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
