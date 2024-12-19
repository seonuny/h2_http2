import os
import sys
import inspect
import threading
import time
from urllib.parse import urlparse
from multiprocessing import Process, Manager

from log   import logger, fixedWidth

class CData:
    def __init__(self):
       #logger.info(f"")
        self.thdId    = None
        self.Id       = None
        self.streamId = None
        self.location = None
        self.headers  = None
        self.payload  = None

    def __str__(self):
        return f"CData[[{self.thdId}_{self.streamId}]:{self.Id}:{self.headers[0] if len(self.headers) > 0 else 'none'}:{urlparse(self.location).path if self.location != None else '/none'}]"

    def __repr__(self):
        return f"CData[[{self.thdId}_{self.streamId}]:{self.Id}:{self.headers[0] if len(self.headers) > 0 else 'none'}:{urlparse(self.location).path if self.location != None else '/none'}]"

    def Set(self,thdId,streamId):
       #logger.info(f"")
        self.thdId = thdId
        self.streamId = streamId

    def Get(self):
       #logger.info(f"")
        return self.thdId, self.streamId

    def IsSame(self,Id,streamId):
        if self.Id == Id and self.streamId  == streamId :
            return True
        else:
            return False

    def SetId(self,Id):
        self.Id = Id

    def GetId(self):
        return self.Id

    def SetLocation(self,location):
       #logger.info(f"")
        self.location = location

    def GetLocation(self):
       #logger.info(f"")
        return self.location

    def SetHeader(self,headers):
       #logger.info(f"")
        self.headers = headers

    def GetHeader(self):
       #logger.info(f"")
        return self.headers

    def SetPayload(self,payload):
       #logger.info(f"")
        self.payload = payload

    def GetPayload(self):
       #logger.info(f"")
        return self.payload

class CReqData:
    def __init__(self):
        self.cvId      = None
        self.streamId  = None
        self.sendType  = None
        self.startTime = None
        self.endTime   = None

    def __repr__(self):
        return f"CReqData:{self.cvId}_{self.streamId}/{self.sendType}/{self.startTime}"

    def SetId(self,Id):
        self.cvId = Id

    def GetId(self):
        return self.cvId 

    def SetStreamId(self,streamId):
        self.streamId = streamId

    def GetStreamId(self):
        return self.streamId

    def SetSendType(self,Type):
        self.sendType = Type

    def GetSendType(self):
        return self.sendType

    def SetStartTime(self,startTime):
        self.startTime = startTime

    def GetStartTime(self):
        return self.startTime

    def SetEndTime(self,endTime):
        self.endTime = endTime

    def GetEndTime(self):
        return self.endTime

    def GetDuration(self):
        return self.endTime - self.startTime

class CElapsed:
    def __init__(self):
        self.start = time.time()
        self.last = self.start
        self.end  = self.last
        self.tot_send = 0
        self.sendTotal = 0
        self.res = dict()
        self.m = dict()
        self.c = dict()
        self.u = dict()
        self.d = dict()

    def __del__(self):
       #logger.info(f"")
        self.logRes()

        self.m.clear()
        self.c.clear()
        self.u.clear()
        self.d.clear()
        self.res.clear()

    def lastLog(self):
        self.log('1')
        self.log('2')
        self.log('3')
        self.log('0','1')
        self.logRes()

    def SetStart(self):
        self.start = time.time()

    def logRes(self):
        res_str = "|"
        tot_cnt = 0
        for k,v in self.res.items():
            tot_cnt += v
            res_str += f"{k:5>s}:{v:10,d}|"
        elaplsed = int(time.time()-self.start)
        tps = int(tot_cnt/elaplsed)
        logger.info(f"elaplsed:{elaplsed:7,}/tps:{tps:5,d}/status:[{self.sendTotal:11,d}-{tot_cnt:11,d}={self.sendTotal-tot_cnt:5,}]/res:{res_str}")

    def logToStr(self,m):
        log_k_str = "|"
        log_v_str = "|"
        cnt       = 0
        for k,v in m.items():
            cnt += v
            log_k_str += f"{k:9d}|"
            log_v_str += f"{v:9,d}|"
        return log_k_str,log_v_str,cnt

    def log(self,t:str='0',last_end='0'):
        l = None
        cnt = 0
        sec = 0
        if   t == '0':
            l = self.m
            logger.info(f"all")
            sec = int(self.end - self.last)
        elif t == '1':
            l = self.c
            logger.info(f"create")
        elif t == '2':
            l = self.u
            logger.info(f"update")
        elif t == '3':
            l = self.d
            logger.info(f"delete")
        k_str,v_str,cnt = self.logToStr(l)
        dash = "-"
        width=(len(l)*10) +1
        logger.info(f"{dash:-<{width}}")
        logger.info(f"{k_str}")
        logger.info(f"{dash:-<{width}}")
        logger.info(f"{v_str}")
        logger.info(f"{dash:-<{width}}")
        if t == '0' :
            if last_end=='0':
                send_cnt = cnt - self.tot_send
                if send_cnt > 0 and sec > 0:
                    logger.info(f"total:{cnt:,d}/tps:{send_cnt:,d}/{sec:,d}={send_cnt/sec:5.2f}")
                self.tot_send = cnt
                self.last = self.end
           #else:
           #    sec = int(self.end - self.start)
           #    if cnt > 0 and sec > 0:
           #        logger.info(f"end:tps:{cnt:,d}/{sec:,d}={cnt/sec:5.2f}")

    def Init(self):
        self.DictInit(self.m)
        self.DictInit(self.c)
        self.DictInit(self.u)
        self.DictInit(self.d)

    def DictInit(self,m):
        m[    1] = 0
        m[    5] = 0
        m[   10] = 0
        m[   50] = 0
        m[   60] = 0
        m[   70] = 0
        m[   80] = 0
        m[   90] = 0
        m[  100] = 0
        m[  200] = 0
        m[  300] = 0
        m[  400] = 0
        m[  500] = 0
        m[ 1000] = 0
        m[ 3000] = 0
        m[ 5000] = 0
        m[10000] = 0
        m[10001] = 0

    def SetReq(self,cnt):
        self.sendTotal += cnt

    def SetRes(self,status):
        try:
            cnt = self.res[status]
            self.res[status] = cnt + 1
        except Exception as e:
            self.res[status] = 1


    def Set(self,duration:float,t:str='0'):
        try:
            self.end = time.time()
            b = False
            kc = None
            vc = None
            for k,v in self.m.items():
                if k > duration*1000:
                    self.m[k] = v + 1
                    b = True
                    break
            if b == False:
                idx = len(self.m) - 1
                key = list(self.m.keys())[idx]
                v = self.m[key]
                self.m[key] = v + 1

            b = False
            if t == '1':
                for k,v in self.c.items():
                    if k > duration*1000:
                        self.c[k] = v + 1
                        b = True
                        break
                if b == False:
                    idx = len(self.c) - 1
                    key = list(self.c.keys())[idx]
                    v = self.c[key]
                    self.c[key] = v + 1

            b = False
            if t == '2':
                for k,v in self.u.items():
                    if k > duration*1000:
                        self.u[k] = v + 1
                        b = True
                        break
                if b == False:
                    idx = len(self.u) - 1
                    key = list(self.u.keys())[idx]
                    v = self.u[key]
                    self.u[key] = v + 1

            b = False
            if t == '3':
                for k,v in self.d.items():
                    if k > duration*1000:
                        self.d[k] = v + 1
                        b = True
                        break
                if b == False:
                    idx = len(self.d) - 1
                    key = list(self.d.keys())[idx]
                    v = self.d[key]
                    self.d[key] = v + 1

        except Exception as e:
            logger.info(f"Error code: {type(e)}-{e},Error on line:{sys.exc_info()[-1].tb_lineno}")

class CCondition:
    def __init__(self,cnt,manager):
        self.Name    = self.__class__.__name__
        self.thdId   = 0
        self.manager = manager
        self.res_map = [ self.manager.dict() for _ in range(cnt) ]
        self.lock    = [threading.Lock() for _ in range(cnt) ]
        self.cv      = [threading.Condition() for _ in range(cnt) ]

    def wait(self,idx,timeout=1):
       #logger.info(f"idx:{idx:02}")
        with self.cv[idx]:
            self.cv[idx].wait(timeout=timeout)

    def notify(self,idx):
       #logger.info(f"idx:{idx:02}")
        with self.cv[idx]:
            self.cv[idx].notify()

    def size(self,idx):
       #plogger.info(f"idx:{idx:02}")
        return len(self.res_map[idx])

    def existKey(self,idx, key):
       #logger.info(f"idx:{idx:02}/[{key}]")
        bExist = key in self.res_map[idx]
        
        if bExist == False:
            self.wait(idx,timeout=1)
            bExist = key in self.res_map[idx]

        return bExist

    def get(self,idx,key):
       #logger.info(f"idx:{idx:02}/key:[{key}]")
        d = None
        try:
            d =  self.res_map[idx].get(key,timeout=2)
        except Exception as e:
            self.res_map[idx].task_done()
        return d

    def pop(self,idx,key):
        with self.lock[idx]:
            b = self.res_map[idx].pop(key)
       #logger.info(f"idx:{idx:02}/[{key}]-{b}")
        return b


    def put(self,idx,key,val):
        with self.lock[idx]:
           #logger.info(f"idx:{idx:02}/[{key}]:{val}")
            self.res_map[idx][key] = val
        self.notify(idx)
        return 

    def printmap(self,idx):
       #logger.info(f" ")
        for key,val in  self.res_map[idx].items():
            logger.info(f"idx:{idx:02}/[{key}:{val}]")

