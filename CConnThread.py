
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
       #logger.info(f"")
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
       #logger.info(f"")
        self.sock.close()
        logger.info(f"remain req:{self.size()}")
        self.request_map.clear()

    def __str__(self):
        return f"CConnThread:{self.thdId}/{self.host}:{self.port}"

    def __repr__(self):
        return f"CConnThread:{self.thdId}/{self.host}:{self.port}"

    def isReady(self):
        return self.bReady

    def stop(self):
       #logger.info(f"")
        self.sock.close()
        self.bRun = False

    def size(self):
       #logger.info(f"")
        return len(self.request_map)

    def existKey(self, key):
       #logger.info(f"")
        bExist = key in self.request_map 
       #logger.info(f"idx:{self.thdId}/[{self.thdId}_{key}]:{bExist}:size:{self.size()}")
        return bExist

    def get(self,key):
       #logger.info(f"")
       #logger.info(f"idx:{self.thdId:02}/{self.thdId}_{key}]")
        return self.request_map.get(key,timeout=2)

    def pop(self,key):
       #logger.info(f"")
       #logger.info(f":{key}")
        with self.lock:
            d = self.request_map.pop(key)
        return d


    def put(self,key,val):
       #logger.info(f"")
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
            stream_id = self.conn.get_next_available_stream_id()
            self.conn.send_headers(stream_id, headers, end_stream=(body is None))
            if body:
                self.conn.send_data(stream_id, body.encode('utf-8'), end_stream=True)
            try:
                conn_remote_size = self.conn.remote_flow_control_window(stream_id)
                if(conn_remote_size< 4096):
                   #logger.info(f"thd:{self.thdId}------------increment_flow_control_window-------------{stream_id}")
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
        try:
            self.sock.sendall(self.conn.data_to_send())
        except  Exception as e:
            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
            self.stop()
           #logger.info(f":[{self.thdId}_{stream_id}]")

        return self.thdId, stream_id

    def recv(self):
       #logger.info(f"")
        events   = None
        data_received = None

        status   =  None
        location = None
        streamId = None
        Payload  = None
        Headers  = list()
        while self.bRun == True:
            try:
                data_received = self.sock.recv(65535)
            except Exception as e:
                logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")

            if data_received is None:
                continue

            events = self.conn.receive_data(data_received)

            try:
               #logger.info(f"[{self.thdId}] {events}")
                for event in events:
                   #logger.info(f"[{self.thdId}_{streamId}] {event}")
                    if isinstance(event, ResponseReceived):
                        location = None
                        streamId = event.stream_id
                        Headers = event.headers
                        for header in Headers:
                            if header[0] == ':status':
                                status = header[1]
                                self.elapsed.SetRes(status)
                            if header[0] == 'location':
                                location = header[1]
                                self.location = location
                        try:
                            if (status == '204' or int(status) >= 300) != True:
                               #logger.info(f"status:{status}")
                                continue
                            for i in range(2):
                                if self.existKey(streamId) == True:
                                    key = f"{self.thdId}_{streamId}"
                                    e = time.time()
                                    d = None
                                    d = self.pop(streamId)
                                   #logger.info(f"{d.GetSendType()}:dur:{e-d.GetStartTime():.2f}")
                                    self.elapsed.Set(e - d.GetStartTime(),d.GetSendType())
                                    data = CData()
                                    data.Set(self.thdId,self.Name)
                                    data.SetId(d.GetId())
                                    data.SetHeader(Headers)
                                   #data.SetPayload(Payload)
                                    if location is not None:
                                        data.SetLocation(location)
                                   #self.response_map[f"{key}"] = data
                                    self.cv.put(d.GetId(),key,data)
                                   #logger.info(f"put:[{key}]:{data}")
                                   #self.cv.notify(d.GetId())
                                    break
                               #else:
                               #    time.sleep(0.01)
                               #    logger.info(f"{streamId} not found in request_map:{self.size()}")
                               #   #self.printmap()
                        except KeyError as e:
                            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                        except Exception as e:
                            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                       #logger.info(f"[{self.thdId}_{streamId}] header")
                    elif isinstance(event, DataReceived):
                        streamId = event.stream_id
                        Payload = event.data
                        try:
                            for i in range(2):
                                if self.existKey(streamId) == True:
                                    key = f"{self.thdId}_{streamId}"
                                    e = time.time()
                                    d = self.pop(streamId)
                                   #logger.info(f"{d.GetSendType()}:dur:{e-d.GetStartTime():.2f}")
                                    self.elapsed.Set(e- d.GetStartTime(),d.GetSendType())
                                    data = CData()
                                    data.Set(self.thdId,streamId)
                                    data.SetId(d.GetId())
                                    data.SetHeader(Headers)
                                    data.SetPayload(Payload)
                                    if location is not None:
                                        data.SetLocation(location)
                                   #self.response_map[f"{key}"] = data
                                    self.cv.put(d.GetId(),key,data)
                                   #if status == '201':
                                   #    logger.info(f"put:[{key}]:{data}")
                                   #self.cv.notify(d.GetId())
                                    break
                                else:
                               #    time.sleep(0.01)
                                    logger.info(f"{streamId} not found in request_map:{self.size()}")
                               #   #self.printmap()
                        except KeyError as e:
                            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                        except FileNotFoundError as e:
                            self.bRun = False
                            return
                        except BrokenPipeError as e:
                            self.bRun = False
                            return
                        except Exception as e:
                            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                       #json_payload = json.loads(Payload)
                       #logger.info(f"[{self.thdId}_{streamId}] {Payload[0:50]}")
                    elif isinstance(event, StreamEnded):
                        continue
                    elif isinstance(event, h2.events.ConnectionTerminated):
                        logger.info(f"event:{event}")
                        return
                    else:
                        if type(event).__name__ not in ['PingReceived','RemoteSettingsChanged','SettingsAcknowledged','StreamEnded','StreamReset','WindowUpdated'
                        ,'ConnectionTerminated']:
                            logger.info(f"[{self.thdId} :{event}")
                self.sock.sendall(self.conn.data_to_send())
            except Exception as e:
                if self.bRun == True:
                    logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
                return 
        return

    """start()시 실제로 실행되는 부분이다"""
    def run(self):
       #logger.info(f"")
       #logger.info(f"start : {self.name}")
        self.bReady = True
        try:
            while self.bRun == True:
               #logger.info(f"-while")
                self.recv()
        except Exception as e:
            logger.info(f"Exceptoin:{type(e)}:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")

       #logger.info(f"{threading.currentThread().getName()} is end")

