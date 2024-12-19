# -*- coding: utf-8 -*-
import os
import sys
import logging
import threading
from logging.handlers import TimedRotatingFileHandler

os.chdir(os.path.dirname(sys.argv[0]))

#log 생성
logger   = logging.getLogger(__name__)

# LWP를 포함한 LogRecordFactory
old_factory = logging.getLogRecordFactory()

def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)
    record.native_id = threading.get_native_id() if hasattr(threading, "get_native_id") else threading.get_ident()
    return record

logging.setLogRecordFactory(record_factory)

#log 레벨 설정
logger.setLevel(logging.INFO)
filename = './LOG/{0}.log'.format(os.path.splitext(os.path.basename(sys.argv[0]))[0])
os.makedirs(os.path.dirname(filename), exist_ok=True)
handler  = TimedRotatingFileHandler(filename, when='midnight', interval=1,  encoding='utf-8')
#formatter = logging.Formatter( '[%(asctime)s.%(msecs)03d] [%(filename)17.17s:%(funcName)18.18s:%(lineno)04d] %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
#formatter = logging.Formatter( '[%(asctime)s.%(msecs)05d][%(filename)15.15s:%(funcName)15.15s:%(lineno)04d:%(thread)05d] %(message)s', datefmt='%H:%M:%S')
#formatter = logging.Formatter( '[%(asctime)s.%(msecs)05d][%(filename)15.15s:%(funcName)15.15s:%(lineno)04d:PID:%(process)d,TID: %(thread)d:%(native_id)05d] %(message)s', datefmt='%H:%M:%S')
#formatter = logging.Formatter( '[%(asctime)s.%(msecs)05d][%(filename)15.15s:%(funcName)15.15s:%(lineno)04d:%(native_id)05d] %(message)s', datefmt='%H:%M:%S')
formatter = logging.Formatter( '[%(asctime)s.%(msecs)05d][%(filename)15.15s:%(lineno)04d:%(native_id)05d] %(message)s', datefmt='%H:%M:%S')
#formatter = logging.Formatter( '[%(asctime)s.%(msecs)05d][%(filename)15.15s:%(funcName)15.15s:%(lineno)04d:%(process)d] %(message)s', datefmt='%H:%M:%S')
handler.setFormatter(formatter)
handler.suffix = '%Y%m%d' 
logger.addHandler(handler)

def fixedWidth(string,width,align="<",fill=" "):
    try:
        enc_len = len(string.encode('euckr'))
        str_len = len(string)
        ko_len  = enc_len - str_len
        if ko_len == 0 or width - ko_len < 0 :
            return f"{string:{fill}{align}{width}}"
        else:
            return f"{string:{fill}{align}{width-ko_len}}"
    except:
        return f"{string:{fill}{align}{width}}"

def preFormat(string, width, align='<', fill=' '):
    count = (width - sum(1 + (unicodedata.east_asian_width(c) in "WF") for c in string))
    return {
        '>': lambda s: fill * count + s, # lambda 매개변수 : 표현식
        '<': lambda s: s + fill * count,
        '^': lambda s: fill * (count / 2)
                       + s
                       + fill * (count / 2 + count % 2)
    }[align](string)
