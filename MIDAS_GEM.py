# Python 3 tool to log data to MIDAS (feLabVIEW)

# Standard libraries:
import sys
import threading
import time
# Get Hostname of machine using python
import socket
import struct
import datetime
import json
import array  # Default behaviour is to use array as data type for logging...
import os
import gzip
# External libraries:

# Numpy is also supported
try:
    import numpy as np
    HaveNumpy = True
    print("Numpy found... np arrays are supported")
except ModuleNotFoundError:
    HaveNumpy = False
    print("Numpy not found... thats ok, but you can only use python \
           arrays for data")

try:
    import psutil
    HavePsutil = True
    print("psutil found... I will log CPU and MEM load")
except ModuleNotFoundError:
    HavePsutil = False
    print("psutil not found... please install it to log the CPU load \
           on this machine (requires python3-devel)")


DataByteOrder = 0
if sys.byteorder == 'little':
    DataByteOrder = 2
elif sys.byteorder == 'big':
    DataByteOrder = 1
else:
    print("Byte order not recognised")
    exit(1)


# Timestamp functions
def GetLVTimeNow():
    # Get UNIX time now
    lvtime = datetime.datetime.utcnow().timestamp()
    # Add seconds between UTC 1/1/1904 and 1/1/1970
    lvtime += 2082844800.0
    # Convert to i64 seconds + u64 fraction of labview
    fraction = lvtime % 1
    seconds = int(lvtime-fraction)
    lvfraction = int(fraction*pow(2, 64))
    # Pack timestamp into 128 bit struct
    LVTimestamp = struct.pack('qQ', seconds, lvfraction)
    return LVTimestamp


# This is hand coded... someone please check my calculation... check timezones?
def GetUnixTimeFromLVTime(timestamp):
    [UnixTime, Fraction] = struct.unpack('qQ', timestamp)
    UnixTime -= 2082844800
    return UnixTime


# Array type parsing functions
def GetArrayType(arg):
    switcher = {
        'd': b"DBL\0",
        'f': b"FLT\0",
        'l': b"I32\0",
        'L': b"U32\0",
    }
    return switcher.get(arg, "Unsupported array type (" + str(arg) +
                        ")... consider using floats?")


def GetNpArrayType(arg):
    switcher = {
        np.dtype('float64'): b"DBL\0",
        np.dtype('float32'): b"FLT\0",
        np.dtype('int32'): b"I32\0",
        np.dtype('uint32'): b"U32\0",
    }
    return switcher.get(arg, "Unsupported numpy array type (" +
                        str(arg) +
                        ")... consider using floats?")


# I only support list of doubles!
def GetListType(arg):
    switcher = {
        type(float()): b"DBL\0",
        # b"I64\0": int,
    }
    return switcher.get(arg, "Unsupported list type (" +
                        str(arg) +
                        ")... consider using floats?")


def CleanString(arg, length):
    if isinstance(arg, str):
        arg = bytes(arg[0:length], 'utf8')
    return arg


# Main DataPacker Object... use it as a global object, its thread safe
class DataPacker:
    # I have list of DataBanks
    RunNumber = -99
    RunStatus = str()
    PeriodicTasks = list()
    BufferOverflowCount = 0
    TestMode = False
    TestModeBuffer = ""
    TestModeWriter = []

    def TurnOnTestMode(self):
        self.TestModeWriter = CompressedCSVWriter()
        self.TestMode = True
    
    def TurnOnDebugMode(self):
        self.AddData(b"THISHOST",
                     b"COMMAND",
                     b"ENABLE_DEBUG_MODE",
                     0,
                     0,
                     GetLVTimeNow(),
                     ""
                     )

    def TurnOffDebugMode(self):
        self.AddData(b"THISHOST",
                     b"COMMAND",
                     b"DISABLE_DEBUG_MODE",
                     0,
                     0,
                     GetLVTimeNow(),
                     ""
                     )

    # Public member functions:
    def AnnounceOnSpeaker(self, category, message):
        self.AddData(category,
                     b"TALK",
                     b"\0",
                     0,
                     0,
                     GetLVTimeNow(),
                     message,
                     True)

    def GetRunNumber(self):
        # Launch the periodic task to track the RunNumber
        self.__AddPeriodicRequestTask("GET_RUNNO")
        # Wait until we have a valid RunNumber (happens on first call only)
        while self.RunNumber < -1:
            time.sleep(0.1)
        return self.RunNumber

    def GetRunStatus(self):
        # Launch the peridoc task to track Run Status
        self.__AddPeriodicRequestTask("GET_STATUS")
        while len(self.RunStatus) == 0:
            time.sleep(0.1)
        return self.RunStatus

    # The public version of AddData can ONLY queue to self.DataBanks
    def AddData(self, category, varname, description, history_settings,
                history_rate, timestamp, data, insert_front=False):
        self.__AddData(category, varname, description, history_settings,
                       history_rate, timestamp, data, self.DataBanks,
                       insert_front)

    # The private version of AddData can use custom queues (ie for __connect())
    def __AddData(self, category, varname, description, history_settings,
                  history_rate, timestamp, data, databanks,
                  insert_front=False):
        # Clean up input strings... (convert str to bytes and trim length)
        category = CleanString(category, 16)
        varname = CleanString(varname, 16)
        description = CleanString(description, 32)
        # Default data type
        TYPE = b"NULL"
        if self.TestMode:
            self.__LogInTestMode(timestamp, category, varname, data)
        # Convert any lists to an array
        if isinstance(data, list):
            TYPE = GetListType(type(data[0]))
            assert TYPE == b"DBL\0", "list support is limited to doubles...\
                                      please use arrays (or np arrays) \
                                      for any other data type!"
            data = array.array('d', data)
        # If we have Numpy, convert array to byte array
        if HaveNumpy:
            # https://docs.scipy.org/doc/numpy/reference/arrays.dtypes.html
            if isinstance(data, np.ndarray):
                # Convert numpy array to byte array
                TYPE = GetNpArrayType(data.dtype)
                assert len(TYPE) == 4, str(TYPE)
                data = data.tobytes()
        # Convert python array to byte array
        # https://docs.python.org/3/library/array.html
        if isinstance(data, array.array):
            TYPE = GetArrayType(data.typecode)
            assert len(TYPE) == 4, str(TYPE)
            # Data need to be encoded as bytes... convert now
            data = data.tobytes()
        # Convert string data to byte array
        elif isinstance(data, str):
            data = bytearray(str(data)+str('\0'), 'utf-8')
            TYPE = b"STR\0"
        # Unknown data type... maybe the user is logging a 'blob' of data
        elif isinstance(data, bytearray) or isinstance(data, bytes):
            if TYPE == b"NULL":
                TYPE = b"U8\0\0"
        else:
            print("Unsupported data format (" +
                  str(type(data)) +
                  ")... upgrade DataPacker!")
            exit(1)
        if varname != b"TALK" and varname != b"COMMAND":
            # Find existing bank to add data to
            for bank in databanks:
                if bank.IsBankMatch(category, varname):
                    # Bank already in memory! Add data to it!
                    bank.AddData(timestamp, data)
                    return
        # Matching bank not found in list... add this new bank to
        # DataBanks list
        bank = DataBank(TYPE,
                        category,
                        varname,
                        description,
                        history_settings,
                        history_rate)
        bank.AddData(timestamp, data)
        if insert_front:
            databanks.insert(0, bank)
        else:
            databanks.append(bank)

    # Private member functions
    def __init__(self, midas_server, port = 12345, max_data_rate = 0):
        self.experiment = midas_server
        self.initial_port = port
        self.port = port
        self.DataBanks = []
        self.BankArrayID = 0
        self.MaxEventSize = max_data_rate
        # Connect to LabVIEW frontend 'supervisor'
        self.__connect()
        self.__run_forever()

    def __connect(self):
        self.PauseLogging = True
        print("Connecting to MIDAS server...")
        # self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.socket.connect((self.experiment,5555))
        print("Connection made... Requesting to start logging")
        ConnectBanks = []
        # Negociate connection to worker frontend
        self.FrontendStatus = ""
        self.address = self.experiment
        self.port = self.initial_port
        while len(self.FrontendStatus) == 0:
            self.MyHostName = socket.gethostname()
            self.__AddData("THISHOST",
                           "COMMAND",
                           "START_FRONTEND",
                           0,
                           0,
                           GetLVTimeNow(),
                           self.MyHostName,
                           databanks=ConnectBanks)
            # Self registration on allowed host list is usually
            # disabled in frontend, so this might do nothing
            self.__AddData("THISHOST",
                           "COMMAND",
                           "ALLOW_HOST",
                           0,
                           0,
                           GetLVTimeNow(),
                           self.MyHostName,
                           databanks=ConnectBanks)
            self.__AddData("THISHOST",
                           "COMMAND",
                           "GIVE_ME_ADDRESS",
                           0,
                           0,
                           GetLVTimeNow(),
                           self.MyHostName,
                           databanks=ConnectBanks)
            self.__AddData("THISHOST",
                           "COMMAND",
                           "GIVE_ME_PORT",
                           0,
                           0,
                           GetLVTimeNow(),
                           self.MyHostName,
                           databanks=ConnectBanks)
            self.__SendWithTimeout(self.__Flush(ConnectBanks), 1000)

        # Connect to LabVIEW frontend 'worker' (where we send data)
        # Request the max data pack size
        if self.MaxEventSize:
            self.__AddData("THISHOST",
                           "COMMAND",
                           "SET_EVENT_SIZE",
                           0,
                           0,
                           GetLVTimeNow(),
                           str(self.MaxEventSize),
                           databanks=ConnectBanks)
        self.MaxEventSize = -1
        while self.MaxEventSize < 0:
            self.__AddData("THISHOST",
                           "COMMAND",
                           "GET_EVENT_SIZE",
                           0,
                           0,
                           GetLVTimeNow(),
                           str("\0"),
                           databanks=ConnectBanks)
            self.__SendWithTimeout(self.__Flush(ConnectBanks))
        print("MaxEventSize:" + str(self.MaxEventSize))
        # Announce I am connection on MIDAS speaker
        connectMsg = "New python connection from " + \
                     self.MyHostName + \
                     " PROGRAM:" + str(sys.argv)
        print(connectMsg)
        self.AnnounceOnSpeaker("THISHOST", connectMsg)
        self.PauseLogging = False

    def __run_forever(self):
        # Start background thread to flush data
        self.KillThreads = False
        self.PauseLogging = False
        self.t1 = threading.Thread(target=self.__Run)
        self.t1.start()
        # Start lightweight background thread to log CPU load
        if HavePsutil:
            self.t2 = threading.Thread(target=self.__LogLoad)
            self.t2.start()
        print("Polling thread launched")

    def __stop(self):
        print("Stopping...")
        self.KillThreads = True
        print("Closing socket")
        # self.socket.disconnect(self.address)
        self.socket.close()
        print("Clearing list")
        self.DataBanks = []
        # self.context.destroy()
        print("done")

    # Log CPU load and memory usage once per minute
    def __LogLoad(self):
        while True:
            CPUMEM = psutil.cpu_percent(interval=60, percpu=True)
            CPUMEM.append(psutil.virtual_memory().percent)
            # print("Logging CPUMEM "+str(CPUMEM))
            self.AddData("THISHOST",
                         "CPUMEM",
                         "",
                         0,
                         10,
                         GetLVTimeNow(),
                         CPUMEM)
            if self.KillThreads:
                break

    # Add a task that is called once per second (eg track RunNumber).
    # (Is private function)
    def __AddPeriodicRequestTask(self, task):
        if task not in self.PeriodicTasks:
            self.PeriodicTasks.append(task)

    # Tool to dump out all logged data to a local file
    def __LogInTestMode(self, timestamp, category, varname, data):
        [LVTime, Fraction] = struct.unpack('qQ', timestamp)
        line = "%s, %s, %s, %s, " % (LVTime,
                                     Fraction,
                                     category.decode("utf-8"),
                                     varname.decode("utf-8"))
        for i in data:
            line += str(i) + str(",")
        line += str("\n")
        self.TestModeBuffer += line
        # If the buffer has more than 10kb in it... write!
        if len(self.TestModeBuffer) > 10000:
            self.TestModeWriter.write(self.TestModeBuffer)
            self.TestModeBuffer = ""

    # Check all banks for data that needs flushing
    def __BanksToFlush(self, databanks):
        n = 0
        for bank in databanks:
            if bank.NumberToFlush() > 0:
                n += 1
        return n

    # Flatten all data in memory (to send to MIDAS)
    def __Flush(self, databanks):
        # Decrement the buffer overflow counter once per second until =0
        if self.BufferOverflowCount > 0:
            self.BufferOverflowCount = self.BufferOverflowCount-1
        # Track remaining buffer space
        buffer_remaining = 10000  # Some default value
        if self.MaxEventSize > 0:
            buffer_remaining = self.MaxEventSize
        # If data packer has no banks... do nothing
        if len(databanks) == 0:
            return
        # If data packer has one bank, flush it
        if len(databanks) == 1:
            return databanks[0].Flush(self, buffer_remaining)
        # If data packer only has one bank type to flush... flush it
        if self.__BanksToFlush(databanks) == 1:
            for bank in databanks:
                if bank.NumberToFlush() > 0:
                    return bank.Flush(self, buffer_remaining)
        # If data packer has many banks to flush, put them in a superbank
        # Track remaining buffer space, less the size of a bank array header
        buffer_remaining = buffer_remaining-16
        print("Building super bank")
        lump = b''
        number_of_banks = 0
        # Loop over all banks and flush each one
        for bank in databanks:
            n_to_flush = bank.NumberToFlush()
            if n_to_flush == 0:
                continue
            bank = bank.Flush(self, buffer_remaining)
            if len(bank):
                buffer_remaining = buffer_remaining-len(bank)
                lump = struct.pack('{}s{}s'.format(len(lump),
                                                   len(bank)),
                                   lump,
                                   bank)
                number_of_banks += 1
        super_bank = struct.pack('4sIII{}s'.format(len(lump)),
                                 b"GEA1",
                                 self.BankArrayID,
                                 len(lump),
                                 number_of_banks,
                                 lump)
        self.BankArrayID = self.BankArrayID+1
        print("Size of lump in super bank:" + str(len(lump)) +
              "(" + str(number_of_banks) + " banks)")
        return super_bank

    # Parse the json string MIDAS sends as a reply to data
    def __HandleReply(self, reply):
        # Unfold the json string into a dictionary
        ReplyList = json.loads(reply)
        # print(ReplyList)
        if 'RunNumber' in ReplyList:
            self.RunNumber = int(ReplyList['RunNumber'])
        if 'EventSize' in ReplyList:
            self.MaxEventSize = int(ReplyList['EventSize'])
        if 'RunStatus' in ReplyList:
            self.RunStatus = ReplyList['RunStatus']
        if 'SendToAddress' in ReplyList:
            self.address = ReplyList['SendToAddress']
        if 'SendToPort' in ReplyList:
            self.port = int(ReplyList['SendToPort'])
        if 'FrontendStatus' in ReplyList:
            self.FrontendStatus = ReplyList['FrontendStatus']
        if 'MIDASTime' in ReplyList:
            self.MIDASTime = float(ReplyList['MIDASTime'])
        if 'msg' in ReplyList:
            print(ReplyList['msg'])
        if 'err' in ReplyList:
            print(ReplyList['err'])

    def __send_block(self, message, response_size, timeout_limit=10.0):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.settimeout(timeout_limit)
        self.socket.connect((self.experiment, self.port))
        self.socket.sendall(message)
        response = b""
        bracket_counter = int(0)
        # Read reponse back
        response += self.socket.recv(response_size)
        # Track brackets, json message is complete when bracket_counter = 0
        bracket_counter += response.count(b"{")
        bracket_counter -= response.count(b"}")
        # Read until end of json message
        while bracket_counter > 0:
            more = self.socket.recv(response_size)
            bracket_counter += more.count(b"{")
            bracket_counter -= more.count(b"}")
            response += more
            # print(response)
        self.socket.shutdown(socket.SHUT_WR)
        self.socket.close()
        return response

    # Send formatted data to MIDAS
    def __SendWithTimeout(self, data, timeout_limit=10.0):
        reply = ""
        try:
            reply = self.__send_block(data, 1024, timeout_limit)
        except socket.timeout:
            # self.AnnounceOnSpeaker("TCPTimeout", "Connection drop detected...")
            print("Failed to send after "+str(timeout_limit)+" seconds")
        except ConnectionResetError:
            print("Connection got reset... trying to again...")
            # self.__connect()
            self.__SendWithTimeout(data, timeout_limit)
        except ConnectionRefusedError:
            print("Connection got refused... trying to connnect...")
            time.sleep(1.)
            if self.port != self.initial_port:
                self.__connect()
            self.__SendWithTimeout(data, timeout_limit)
        except OSError:
            print("OSError... check firewall settings of MIDAS server")
        except Exception:
            print("New unknown exception!!!", sys.exc_info()[0])
            exit(1)
        if len(reply):
            self.__HandleReply(reply)
            if reply[0:5] == b"ERROR":
                print("ERROR reported from MIDAS! FATAL!")
                os._exit(1)
        # print("Sent on attempt"+str(send_attempt))
        print("Data sent and received reply:"+str(reply))
        return

    def CheckDataLength(self, length):
        if length > self.MaxEventSize:
            print("Safety limit! \
                  You are logging too much data too fast (" +
                  str(length/1000) + "kbps>" +
                  str(self.MaxEventSize/1000) +
                  "kbps)... increase this threshold in the odb")
            os._exit(1)

    # Main (forever) loop for flushing the queues... run as its own thread
    def __Run(self, periodic_flush_time=1):
        sleep_time = periodic_flush_time
        # Run forever!
        while True:
            if self.PauseLogging:
                time.sleep(0.1)
                continue
            packing_start = time.time()
            # Execute periodic tasks (RunNumber tracking etc)
            for task in self.PeriodicTasks:
                self.AddData(b"THISHOST",
                             "COMMAND",
                             bytes(task, 'utf-8'),
                             0,
                             0,
                             GetLVTimeNow(),
                             str("\0"))
            # Flatten data in memory and send to MIDAS (if there is any data)
            n = self.__BanksToFlush(self.DataBanks)
            if n > 0:
                Bundle = self.__Flush(self.DataBanks)
                packing_stop = time.time()
                self.CheckDataLength(len(Bundle))
                self.percent_time_packing = \
                    100. * (packing_stop - packing_start) / sleep_time
                print("Packing time percentage:" +
                      str(self.percent_time_packing) + "%")
                # if (self.percent_time_packing>100.):
                #    self.AnnounceOnSpeaker("THISHOST",
                #                           "Warning: \
                #                           Packing time exceeds 100%")
                # print("sleeping:" +
                #       str(sleep_time - (packing_stop - packing_start)))
                wait_time = sleep_time-(packing_stop-packing_start)
                if wait_time > 0:
                    time.sleep(wait_time)
                print("Sending " + str(n) +
                      " banks of data (" + str(len(Bundle)) + " bytes)...")
                # self.socket.send(Bundle)
                # print("Sent...")
                self.__SendWithTimeout(Bundle, 10.0)
                continue
            else:
                print("Nothing to flush")
            if self.KillThreads:
                break
            time.sleep(sleep_time)


class DataBank:
    # LVBANK and LVDATA description:
    # https://alphacpc05.cern.ch/elog/ALPHA/25025
    LVBANKHEADERSIZE = 88
    # LVBANK Header format
    LVBANK = '4s4s16s16s32shhhhii{}s'
    # LVDATA Header format
    LVDATA = '16s{}s'
    r = threading.RLock()

    # Arguments must be bytes... assert statements enforce this
    def __init__(self, datatype, category, varname, eqtype,
                 rate_settings, rate):
        self.BANK = b"GEB1"
        assert isinstance(datatype, bytes)
        self.DATATYPE = datatype
        assert isinstance(category, bytes)
        self.VARCATEGORY = category
        assert isinstance(varname, bytes)
        self.VARNAME = varname
        assert isinstance(eqtype, bytes)
        self.EQTYPE = eqtype
        self.HistorySettings = rate_settings
        self.HistoryRate = rate
        self.DataList = []

    def IsBankMatch(self, category, varname):
        if self.VARCATEGORY == category:
            if self.VARNAME == varname:
                return True
            else:
                return False
        else:
            return False

    def print(self):
        print("BANK:"+str(self.BANK))
        print("TYPE:"+str(self.DATATYPE))
        print("CATEGORY:"+str(self.VARCATEGORY))
        print("VARNAME:"+str(self.VARNAME))
        print("EQTYPE:"+str(self.EQTYPE))
        print("Datalist size:"+str(len(self.DataList)))
        if len(self.DataList):
            print("LVDATA size:"+str(len(self.DataList[0])))

    # Add a single array (LVDATA) of data to the bank (LVBANK)
    def AddData(self, timestamp, data):
        # Pack timestamp and data array into LVDATA format
        lvdata = struct.pack(self.LVDATA.format(len(data)), timestamp, data)
        # Check the length of the last array matches the first
        self.r.acquire()
        if len(self.DataList) > 0:
            assert len(self.DataList[0]) == len(lvdata)
        # Add this LVDATA to a list for later flattening (thread safe)
        self.DataList.append(lvdata)
        self.r.release()

    # Number of items in DataList (Count of arrays logged to bank)
    def NumberToFlush(self):
        return len(self.DataList)

    # Total size of all data waiting to be flattened
    def DataLengthOfBank(self):
        n = self.LVBANKHEADERSIZE
        for bank in self.DataList:
            n += len(bank)
        return n

    # Flatten all data in DataList
    def Flush(self, caller, buffer_remaining):
        self.r.acquire()
        # Check if there is anything to do
        if len(self.DataList) == 0:
            print("Nothing in DataList to flush")
            self.r.release()
            return
        # print("Banks to flush:" + str(self.NumberToFlush() ) +
        #       " Data length:" + str(self.DataLengthOfAllBank()))
        lump = b''
        LocalList = self.DataList
        self.DataList = []
        self.r.release()
        # Remove space needed for header
        buffer_remaining -= 88
        block_size = len(LocalList[0])
        num_blocks = 0
        # Unfold data in DataList list, stop if we run out of buffer
        while buffer_remaining > block_size and len(LocalList):
            lump = struct.pack('{}s{}s'.format(len(lump),
                                               block_size),
                               lump,
                               LocalList.pop(0))
            buffer_remaining -= block_size
            if len(lump):
                num_blocks += 1
        # If we didn't unfold everything, then put it back in the DataList
        if len(LocalList) > 0:
            print("Overflow prevented (" +
                  str(caller.BufferOverflowCount) +
                  ")")
            # caller.AnnounceOnSpeaker("THISHOST",
            #                          "Event Buffer Overflow prevented")
            caller.BufferOverflowCount += 1
            if caller.BufferOverflowCount > 100:
                caller.AnnounceOnSpeaker("THISHOST",
                                         "DataPacker on " +
                                         caller.MyHostName +
                                         " limited by data rate for \
                                         more than a minute")
                caller.BufferOverflowCount = 0
            self.r.acquire()
            while len(LocalList) > 0:
                self.DataList.append(LocalList.pop(0))
            self.r.release()

        # Dimensions of LVDATA in BANK
        if num_blocks == 0:
            return b''
        # self.print()
        # All items in DataList used, clear the memory
        LocalList.clear()
        # Build entire bank with header
        BANK = struct.pack(self.LVBANK.format(len(lump)),
                           self.BANK,
                           self.DATATYPE,
                           self.VARCATEGORY,
                           self.VARNAME,
                           self.EQTYPE,
                           self.HistorySettings,
                           self.HistoryRate,
                           DataByteOrder,  # Timestamp byte order
                           DataByteOrder,  # Data byte order
                           block_size,
                           num_blocks,
                           lump)
        return BANK


class CompressedCSVWriter:

    def __init__(self):
        fname = "MIDAS_GEM_LOG_"
        now = datetime.datetime.now()
        fname += "%04.d%02.d%02d-%02d%02d%02d" % (now.year,
                                                  now.month,
                                                  now.day,
                                                  now.hour,
                                                  now.minute,
                                                  now.second)
        self.fileout = gzip.open(fname+'.csv.gz', 'wb')
        title = bytes('LabVIEW Time (Seconds),' +
                      'Fractions (2^64),' +
                      'Category,' +
                      'Varname,' +
                      'Data...', 'utf-8')
        print(title)
        self.fileout.write(title)

    def write(self, many_lines):
        print("Writing 10kb out compressed CSV")
        # print(many_lines)
        self.fileout.write(bytearray(many_lines, 'utf-8'))
        self.fileout.flush()
