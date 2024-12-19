# -*- coding: utf-8 -*-

import os
import sys
import inspect

import threading
from threading import Thread

import socket
import json
import time
import queue
from urllib.parse import urlparse
from multiprocessing import Process, Manager
from CConnThread import CConnThread
from CCondition import CCondition,CData, CReqData, CElapsed

from log   import logger, fixedWidth


class CWorkerRecv(Thread):
    def __init__(self,thdId,cv,cond,request_queue,response_queue):
        super().__init__()
        self.cv      = cv
        self.cond    = cond
        self.thdId   = thdId
        self.request_queue = request_queue
        self.response_queue = response_queue
        self.bRun    = True

    def __del__(self):
        self.bRun = False
       #logger.info("")

    def __str__(self):
        return f"CWorkerRecv:{self.thdId}"

    def __repr__(self):
        return f"CWorkerRecv:{self.thdId}"

    def stop(self):
        self.bRun = False
        try:
            self.request_queue.put(None)
        except Exception as e:
            None

    def run(self):
        while self.bRun == True:
            self.receive_check()
       #logger.info("end")

    def receive_check(self):
        data = None
        bCrtLoop = True
        while self.bRun == True and bCrtLoop == True:
            try:
                key = None
                try:
                    key = self.request_queue.get(timeout=2)
                except EOFError as e:
                    self.bRun = False
                except queue.Empty as e:
                    continue
                if key == None:
                    if self.bRun != False:
                        time.sleep(0.01)
                    continue
                bExist = self.cv.existKey(self.thdId,key)
                if bExist == True:
                    data = self.cv.pop(self.thdId,key)
                    if data is not None:
                        self.response_queue.put(data)
                        with self.cond:
                            self.cond.notify()
                        bCrtLoop = False
                    else:
                        logger.info(f"idx:{self.thdId:02}/{key}-not data")
                        return None
                else:
                    logger.info(f"idx:{self.thdId:02}/{key}")

            except BrokenPipeError as e:
                self.bRun = False
                return None
            except ConnectionResetError as e:
                self.bRun = False
                return None
            except EOFError as e:
                self.bRun = False
                return None
            except KeyError as e:
                logger.info(f"Exception:{type(e)}:{e}-Error on line:{sys.exc_info()[-1].tb_lineno}")
                time.sleep(0.1)
                return None
            except AttributeError as e:
                logger.info(f"Exception:{type(e)}:{e}-Error on line:{sys.exc_info()[-1].tb_lineno}")
                time.sleep(0.1)
                return None
            except Exception as e:
                logger.info(f"Exception:{type(e)}:{e}-Error on line:{sys.exc_info()[-1].tb_lineno}")
                time.sleep(0.1)
                return None
            except :
                logger.info(f"{sys.exc_info()[0]}/Error on line:{sys.exc_info()[-1].tb_lineno}")
                time.sleep(0.1)
                return None
        return None

