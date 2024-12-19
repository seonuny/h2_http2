# -*- coding: utf-8 -*-

import os
import sys
import inspect

import threading
from threading import Thread

import socket
import json
import time
from urllib.parse import urlparse
from multiprocessing import Process, Manager
from CConnThread import CConnThread
from CCondition import CCondition,CData, CReqData, CElapsed

from log   import logger, fixedWidth

# 데이터 파일 경로
create_file_path = "create.json"
update_file_path = "update.json"
sm_policy_control_path = '/npcf-smpolicycontrol/v1/sm-policies'

def load_data_from_file(file_path):
    """
    JSON 파일에서 데이터를 읽어오는 함수
    :param file_path: JSON 파일 경로
    :return: 파일에서 읽은 데이터 (문자열)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        logger.info(f"파일을 읽는 도중 오류 발생: {e}")
        return None

class CWorker(Thread):

    def __init__(self,Id,Name,workRecv,response_map,request_queue,response_queue,connThdlst,cv,cond,host):
        super().__init__()
        self.thdId   = Id
        self.Name    = Name
       #logger.info(f"")
        self.host    = host['ip']
        self.port    = host['port']
        self.upd_cnt = host['update_cnt']
        self.loop_cnt= host['loop_cnt']
        self.tps     = host['tps']
        self.proc_time = host['proc_time']
        self.init    = None
        self.user_max= host['user_count']
        self.user_idx= 0
        self.mdn     = f'010{self.user_idx:03}{self.thdId:04}'
        self.send_cnt= 0
        self.cv      = cv
        self.cond    = cond
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.response_map = response_map
        self.connThdlst = connThdlst
        self.maxConn = len(self.connThdlst)
        self.idx     = self.thdId % self.maxConn  
        self.create  = None 
        self.update  = None 
        self.data    = None
        self.ctemp   = None
        self.utemp   = None
        self.loadDataFromFile()
        self.bRun    = True
        self.location = ""
        self.workRecv= workRecv

    def __del__(self):
       #logger.info(f"")
        try:
            for i in range(self.request_queue.qsize()):
                self.request_queue.get(timeout=1)
        except FileNotFoundError as e:
            return
        except Exception as e:
            logger.info(f"Exception:{type(e)}:{e}-Error on line:{sys.exc_info()[-1].tb_lineno}")
            self.request_queue.task_done()

    def __str__(self):
        return f"CWorker:{self.thdId}/{self.host}:{self.port}:{self.upd_cnt}/{self.idx}"

    def __repr__(self):
        return f"CWorker:{self.thdId}/{self.host}:{self.port}:{self.upd_cnt}/{self.idx}"

    def stop(self):
       #logger.info(f"")
        self.bRun = False
        try:
            self.request_queue.put(None)
        except Exception as e:
            None

    def loadDataFromFile(self):
       #logger.info(f"")
        # JSON 데이터를 파일에서 읽기
        self.ctemp = load_data_from_file(create_file_path)
        if not self.ctemp:
            raise ValueError("데이터 파일을 읽을 수 없습니다.")

        self.utemp = load_data_from_file(update_file_path)
        if not self.utemp:
            raise ValueError("데이터 파일을 읽을 수 없습니다.")
    
    def setHeader(self,path):
        headers = [
            (':method', 'POST'),
            (':authority', f"{self.host}:{self.port}"),
            (':scheme', 'http'),
            (':path', f'{path}'),
            ('content-type', 'application/json')
           #('content-length', str(len(self.create)))
        ]
        return headers

    def send_data(self,headers, body=None,sendType='2'):
       #logger.info(f"")
        if self.idx >= self.maxConn:
            self.idx = 0
        thdId, streamId = self.connThdlst[self.idx].send(self.thdId,headers,body,sendType) 
        self.idx = self.idx + 1
       #logger.info(f" key:[{thdId}_{streamId}]")
        try:
            self.request_queue.put(f"{thdId}_{streamId}")
        except Exception as e:
            None
        return thdId, streamId

    def wait(self,timeout=1):
        for i in range(10):
            with self.cond:
               #logger.info(f"idx:{self.thdId:02}-{self.cond}-b")
                try:
                    self.cond.wait(timeout)
                   #logger.info(f"idx:{self.thdId:02}-{self.cond}-a")
                    break;
                except Exception as e:
                    None
                   #logger.info(f"idx:{self.thdId:02}-{self.cond}-{type(e)}-{e}")



    def send_create_request(self):
       #logger.info(f"")
        self.create = self.ctemp.replace('$MIN$',f'105{self.thdId:03}{self.user_idx:04}').replace('$IP$',f'{self.Name:10}{self.thdId:03}')
        self.update = self.utemp.replace('$MIN$',f'105{self.thdId:03}{self.user_idx:04}').replace('$IP$',f'{self.Name:10}{self.thdId:03}')

        headers = self.setHeader(sm_policy_control_path)

        thdId , streamId = self.send_data(headers, self.create,'1')
        self.wait()
        return thdId, streamId

    def send_update_request(self,location):
       #logger.info(f"")
    
        parsed_url = urlparse(location)
        path = f"{parsed_url.path}/update"

        headers = self.setHeader(path)

        thdId , streamId = self.send_data(headers,self.update,'2')
        self.wait()
        return thdId, streamId
    

    def send_delete_request(self,location):
       #logger.info(f"")
    
        parsed_url = urlparse(location)
        path = f"{parsed_url.path}/delete"

        headers = self.setHeader(path)

        thdId , streamId = self.send_data(headers,None,'3')
        self.wait()
        return thdId, streamId

    def create_process(self):
        thdId = self.thdId
       #logger.info(f"")
        self.data = None
        Id,streamId = self.send_create_request()
        self.data = None
        try:
            self.data = self.response_queue.get()
        except Exception as e:
            self.data = None


    def update_process(self,i):
        thdId = self.thdId
       #logger.info(f"idx:{i}")
        if self.data is not None:
            Id, streamId = self.send_update_request(self.data.GetLocation())
            try:
                data = self.response_queue.get()
            except Exception as e:
                data = None
        else:
            None
    
    def delete_process(self):
        thdId = self.thdId
        if self.data is not None:
            Id, streamId = self.send_delete_request(self.data.GetLocation())
            data = None
            try:
                data = self.response_queue.get()
            except Exception as e:
                data = None
        else:
            None

    def run(self):
        try:
           #logger.info(f"")
            loop_cnt = self.loop_cnt
            s_time = time.time()
            self.init =s_time
            while self.bRun == True:
                e_time = time.time()
                if self.proc_time != 0:
                    if e_time - self.init > self.proc_time:
                        self.stop()
                        time.sleep(0.1)
                        self.bRun = False
                        break
                gap_time = int(e_time - s_time)
                if self.send_cnt > 0 and (self.send_cnt > self.tps or gap_time >= 1):
                    if gap_time < 1:
                        time.sleep(1-gap_time)
                        s_time = e_time
                        self.send_cnt = 0
               #if loop_cnt % 1000 == 0:
               #    logger.info(f"while-loop_cnt:{loop_cnt}")
                if self.loop_cnt != 0:
                    loop_cnt -= 1
                    if loop_cnt == 0:
                       #logger.info(f"loop_cnt:{loop_cnt} break")
                        self.stop()
                        break
               #time.sleep(1)
                if self.user_idx >= self.user_max:
                    self.user_idx = 0
                self.create_process()
                self.send_cnt = self.send_cnt + 1
                for i in range(self.upd_cnt):
                    self.update_process(i)
                    self.send_cnt = self.send_cnt + 1
                self.delete_process()
                self.send_cnt = self.send_cnt + 1
                self.user_idx = self.user_idx + 1

        except KeyboardInterrupt as e:
            logger.info(f"err:{e},Error on line:{sys.exc_info()[-1].tb_lineno}")
       #logger.info(f"end")
