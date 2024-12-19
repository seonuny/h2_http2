
import os
import sys
import inspect

import json
import socket

from queue import Queue

import threading
from threading import Thread

import h2
from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import ResponseReceived, DataReceived, StreamEnded

from CCondition import CCondition,CData, CReqData, CElapsed

import time

from log   import logger, fixedWidth

"""클래스 생성시 threading.Thread를 상속받아 만들면 된다"""
class CConnThread(Thread):

    #"""__init__ 메소드 안에서 threading.Thread를 init한다"""
    def __init__(self, Id, Name,response_map,elapsed,cv,host):
        super().__init__()
        self.thdId = Id
        self.Name  = Name
        self.bRun  = True
        self.lock = threading.Lock() 
        self.cv    = cv
        self.bReady= False
        # 서버 URL과 포트 설정
        self.response_map = response_map
        self.elapsed = elapsed
        self.request_map  = dict()
        self.status   =  None
        self.location = None
        self.streamId = None
        self.Payload  = None
        self.Headers  = list()

        try:
            self.host  = host[0]
            self.port  = host[1]
            self.sock  = socket.create_connection((self.host, self.port))
            self.config = H2Configuration(client_side=True,header_encoding='utf-8')
            self.conn  = H2Connection(config=self.config)
            self.conn.initiate_connection()
            self.sock.sendall(self.conn.data_to_send())
        except  Exception as e:
            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")

    def __del__(self):
        self.sock.close()
        self.request_map.clear()

    def __str__(self):
        return f"CConnThread:{self.thdId}/{self.host}:{self.port}"

    def __repr__(self):
        return f"CConnThread:{self.thdId}/{self.host}:{self.port}"

    def isReady(self):
        return self.bReady

    def stop(self):
       #logger.info(f"idx:{self.thdId:02}")
        self.sock.close()
        self.bRun = False

    def size(self):
        return len(self.request_map)

    def existKey(self, key):
        bExist = key in self.request_map 
       #logger.info(f"idx:{self.thdId}/[{self.thdId}_{key}]:{bExist}:size:{self.size()}")
        return bExist

    def get(self,key):
       #logger.info(f"idx:{self.thdId:02}/{self.thdId}_{key}]")
        return self.request_map.get(key,timeout=2)

    def pop(self,key):
       #logger.info(f"idx:{self.thdId}/[{self.thdId}_{key}")
        with self.lock:
            d = self.request_map.pop(key)
        return d


    def put(self,key,val):
       #logger.info(f"idx:{self.thdId}/[{self.thdId}_{key}]-{val}")
        with self.lock:
            b = self.request_map[key] = val
        return b

    def printmap(self):
       #logger.info(f"")
        for key,val in  self.request_map.items():
            logger.info(f"idx:{self.thdId}/[{self.thdId}_{key}]-val:{val}")

    def send(self,cvId,headers, body = None,sendType='2'):
        with self.lock:
           #logger.info(f"")
            try:
                stream_id = self.conn.get_next_available_stream_id()
                self.conn.send_headers(stream_id, headers, end_stream=(body is None))
                if body:
                    self.conn.send_data(stream_id, body.encode('utf-8'), end_stream=True)
                self.sock.sendall(self.conn.data_to_send())
            except  Exception as e:
                logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                self.stop()
            try:
                conn_remote_size = self.conn.remote_flow_control_window(stream_id)
                if(conn_remote_size< 4096):
                    self.conn.increment_flow_control_window(1024*1024)
            except Exception as e:
                error = e
                logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
            d = CReqData()
            d.SetId(cvId)
            d.SetStreamId(stream_id)
            d.SetStartTime(time.time())
            d.SetSendType(sendType)
        self.put(stream_id,d)
        self.elapsed.SetReq(1)
        return self.thdId, stream_id

    def recv(self):
       #logger.info(f"idx:{self.thdId}")
        events   = None
        data_received = None

        while self.bRun == True:
            self.sock.sendall(self.conn.data_to_send())
            try:
                data_received = self.sock.recv(65535)
            except Exception as e:
                logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                self.bRun = False
                continue

            if data_received is None:
                continue

            events = self.conn.receive_data(data_received)

            try:
                for event in events:
                    if isinstance(event, ResponseReceived):
                        self.location = None
                        self.streamId = event.stream_id
                        self.Headers = event.headers
                        for header in self.Headers:
                            if header[0] == ':status':
                                self.status = header[1]
                                self.elapsed.SetRes(self.status)
                            if header[0] == 'location':
                                self.location = header[1]
                        try:
                            if (self.status == '204' or int(self.status) >= 300) != True:
                                continue
                            if self.existKey(self.streamId) == True:
                                key = f"{self.thdId}_{self.streamId}"
                                e = time.time()
                                d = None
                                d = self.pop(self.streamId)
                                self.elapsed.Set(e - d.GetStartTime(),d.GetSendType())
                                data = CData()
                                data.Set(self.thdId,self.Name)
                                data.SetId(d.GetId())
                                data.SetHeader(self.Headers)
                                if self.location is not None:
                                    data.SetLocation(self.location)
                                self.cv.put(d.GetId(),key,data)
                        except FileNotFoundError as e:
                            self.bRun = False
                            return
                        except ConnectionResetError as e:
                            self.bRun = False
                            return
                        except BrokenPipeError as e:
                            self.bRun = False
                            return
                        except KeyError as e:
                            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                        except Exception as e:
                            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                       #logger.info(f"[{self.thdId}_{self.streamId}] header")
                    elif isinstance(event, DataReceived):
                        self.streamId = event.stream_id
                        self.Payload = event.data
                        try:
                            if self.existKey(self.streamId) == True:
                                key = f"{self.thdId}_{self.streamId}"
                                e = time.time()
                                d = self.pop(self.streamId)
                               #logger.info(f"{d.GetSendType()}:dur:{e-d.GetStartTime():.2f}")
                                self.elapsed.Set(e- d.GetStartTime(),d.GetSendType())
                                data = CData()
                                data.Set(self.thdId,self.streamId)
                                data.SetId(d.GetId())
                                data.SetHeader(self.Headers)
                                data.SetPayload(self.Payload)
                                if self.location is not None:
                                    data.SetLocation(self.location)
                               #self.response_map[f"{key}"] = data
                                self.cv.put(d.GetId(),key,data)
                               #if self.status == '201':
                               #    logger.info(f"put:[{key}]:{data}")
                               #self.cv.notify(d.GetId())
                        except FileNotFoundError as e:
                            self.bRun = False
                            return
                        except ConnectionResetError as e:
                            self.bRun = False
                            return
                        except BrokenPipeError as e:
                            self.bRun = False
                            return
                        except KeyError as e:
                            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                        except Exception as e:
                            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                    elif isinstance(event, StreamEnded):
                        self.status   = None
                        self.location = None
                        self.streamId = None
                        self.Payload  = None
                        self.Headers.clear()
                        continue
                    elif isinstance(event, h2.events.ConnectionTerminated):
                        logger.info(f"idx:{self.thdId:02}-event:{event}")
                        return
                    else:
                        if type(event).__name__ not in [
                                 'PingReceived'
                                ,'RemoteSettingsChanged'
                                ,'SettingsAcknowledged'
                                ,'StreamEnded'
                                ,'StreamReset'
                                ,'WindowUpdated'
                                ,'ConnectionTerminated'
                                ]:
                            logger.info(f"idx:{self.thdId:02}-event:{event}")
            except Exception as e:
                if self.bRun == True:
                    logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                return 
        return

    """start()시 실제로 실행되는 부분이다"""
    def run(self):
       #logger.info(f"idx:{self.thdId:02} is start")
        self.bReady = True
        try:
            while self.bRun == True:
                self.recv()
        except Exception as e:
            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")

       #logger.info(f"idx:{self.thdId:02} is end")

