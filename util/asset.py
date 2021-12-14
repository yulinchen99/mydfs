import socket 
from common import *
import pickle
import json

def get_free_port():  
    sock = socket.socket()
    sock.bind(('', 0))
    ip, port = sock.getnameinfo()
    sock.close()
    return port

def create_sock(host, port):
    sock = socket.socket()
    sock.connect((host, port))
    return sock

def send_data(socket, data):
    # data = bytes(json.dumps(data), encoding='utf-8')
    try:
        sent = 0
        while sent < len(data):
            sent_part = socket.send(data[sent:sent+BUF_SIZE])
            sent += sent_part
        socket.close()
        return True
    except:
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
        if len(part) == 0:
            # either 0 or end of data
            break
    return chunk_data