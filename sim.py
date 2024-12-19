#!/bin/python3
"""
"""
# -*- coding: utf-8 -*-
import os
import sys
import inspect
from signal       import signal, alarm, SIGALRM 

import threading
from threading import Thread

import socket
import json
import time
#from queue import Queue
#from multiprocessing import Process, Queue
from multiprocessing import Process, Manager
from urllib.parse import urlparse
from CConnThread import CConnThread
from CWorkerThread import CWorker
from CWorkerRecvThread import CWorkerRecv
from CCondition import CCondition,CData, CReqData, CElapsed

from log   import logger, fixedWidth

# 사용자 정의 에러
class ExceptionAlarm(Exception):
        pass 

def sigAlarm(signum, frame):
    raise ExceptionAlarm(f"alarm.signal {signum} : {frame}")

conf_file_path   = "conf.json"

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

class CTPSCheck(Thread):

    def __init__(self,elapsed,proc_time=60):
        super().__init__()
        self.elapsed = elapsed
        self.proc_time = proc_time
        self.init      = None
        self.bRun    = True

    def __del__(self):
        self.bRun = False

    def stop(self):
        self.bRun = False

    def run(self):
        start = time.time()
        self.init = start
        res_start = start
        while self.bRun == True:
            time.sleep(0.1)
            end = time.time()
            if end - self.init > self.proc_time:
                self.bRun = False

            if end - res_start > 1:
                self.elapsed.logRes()
                res_start = end

           #if end - start > 60:
           #    self.elapsed.log()
           #    start = end
       #logger.info("end")
        self.elapsed.lastLog()
        return

def main(argc,argv):
    tpsCheck = None
    try:
       #logger.info(f" ")
        data_str     = load_data_from_file(conf_file_path)
        host         = json.loads(data_str)
        ip           = host['ip']
        port         = host['port']
        worker_count = host['worker_cnt']
        conn_count   = host['conn_cnt']
        proc_time    = host['proc_time']

        logger.info(f"{host}")

        manager      = Manager()
        response_map = manager.dict()

        elapsed = CElapsed()
        elapsed.Init()

        worker_cnt = 1
        conn_cnt = 1
        if worker_count >= 0:
            worker_cnt = worker_count
        if conn_count > 0:
            conn_cnt = conn_count

        request_queue = [  manager.Queue() for _ in range(worker_cnt) ]
        response_queue = [  manager.Queue() for _ in range(worker_cnt) ]
        cond = [ threading.Condition() for _ in range(worker_cnt) ]
        cv = CCondition(worker_cnt,manager)
        connThdlst = list()
        for i in range(conn_cnt) :
            connThd = CConnThread(Id=i,Name="conn",response_map=response_map,elapsed=elapsed,cv=cv,host=(ip,port))
            connThdlst.append(connThd)

        for connThd in connThdlst:
            connThd.start()

        for connThd in connThdlst:
            for i in range(10):
                if connThd.isReady() == True:
                    break
                else:
                    logger.info("Run Ready sleep")
                    time.sleep(1)

        workerlst = list()
        for i in range(worker_cnt):
            workRecv= CWorkerRecv(i,cv=cv,cond=cond[i],request_queue=request_queue[i],response_queue=response_queue[i])
            worker = CWorker(Id=i,Name="worker",workRecv=workRecv,response_map=response_map,request_queue=request_queue[i],response_queue=response_queue[i],connThdlst = connThdlst,cv=cv,cond=cond[i],host=host)
            workerlst.append((worker,workRecv))

        elapsed.SetStart()

        for worker in workerlst:
            worker[0].start()
            worker[1].start()

        tpsCheck = CTPSCheck(elapsed=elapsed,proc_time=proc_time)
        tpsCheck.start()

        for worker in workerlst:
            worker[0].join()
            worker[1].stop()

        for worker in workerlst:
            worker[1].join()

        tpsCheck.stop()

        for connThd in connThdlst:
            connThd.stop()

        for connThd in connThdlst:
            connThd.join()
    
    except KeyboardInterrupt as e:
       #logger.info(f"err:{type(e)}:{e}-Error on line:{sys.exc_info()[-1].tb_lineno}")
        logger.info(f"stop [signal:{type(e)}]")
        tpsCheck.stop()
        for worker in workerlst:
            worker[0].stop()
            worker[1].stop()

        for worker in workerlst:
            worker[1].join()
            worker[0].join()


        for connThd in connThdlst:
            connThd.stop()

        for connThd in connThdlst:
            connThd.join()


   #logger.info("프로그램 종료 중...")

   #logger.info(f"{elapsed}")
    workerlst.clear()
    connThdlst.clear()
   #logger.info("종료")
    return None

if __name__ == '__main__':
    main(len(sys.argv),sys.argv)
    sys.exit()
