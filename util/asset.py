import socket 
import sys
sys.path.append('../')
from common import *
import pickle
import json
from collections import Counter
from contextlib import closing

def get_free_port():
    """ Get free port"""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s: 
        s.bind(('', 0)) 
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        return s.getsockname()[1] 

def create_sock(host, port):
    sock = socket.socket()
    sock.connect((host, port))
    return sock

def send_data(socket, data, close=True):
    # data = bytes(json.dumps(data), encoding='utf-8')
    try:
        sent = 0
        while sent < len(data):
            sent_part = socket.send(data[sent:sent+BUF_SIZE])
            sent += sent_part
        if close:
            socket.close()
        return True
    except:
        if close:
            socket.close()
        return False

def deserialize_data(data):
    return json.loads(str(data, encoding='utf-8'))

def serialize_data(data):
    return bytes(json.dumps(data), encoding='utf-8')

def receive_data(sock):
    chunk_data = b''
    while True:
        part = sock.recv(BUF_SIZE * 2)
        chunk_data += part
        if len(part) == 0 and len(chunk_data) > 0:
        # if len(part) == 0:
            # either 0 or end of data
            break
    return chunk_data

def count_word_dict(s):
    s = s.lower()
    if not s:
        return 0
    words = s.split()
    return dict(Counter(words))
