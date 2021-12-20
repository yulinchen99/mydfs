import os
import socket

from numpy import heaviside

from common import *
import time
import threading
import hashlib
import json
from io import StringIO
import pandas as pd
import numpy as np
import pickle
import multiprocessing

from util.asset import count_word_dict, receive_data, send_data, serialize_data

not_applicable = [0, 1, 5]
word_count = [2, 4, 7]
number = [3, 6]
rate = [8]

# DataNode支持的指令有:
# 1. load 加载数据块
# 2. store 保存数据块
# 3. rm 删除数据块
# 4. format 删除所有数据块

"""TODO 
1. communication between datanodes
2. task specific api
"""

class DataNode:
    def run(self):
        t = threading.Thread(target=self.heartbeat)
        t.start()    

        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", data_node_port))
            listen_fd.listen(5)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("Received request from {}".format(addr))
                
                try:
                    # 获取请求方发送的指令
                    request = str(sock_fd.recv(BUF_SIZE), encoding='utf-8')
                    request = request.split()  # 指令之间使用空白符分割
                    print(request)
                    
                    cmd = request[0]  # 指令第一个为指令类型
                    response = None
                    if cmd == "load":  # 加载数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        # response = self.load(dfs_path)
                        m = threading.Thread(target=self.load_multi, args=(dfs_path, sock_fd))
                        m.daemon = True  # daemon True设置为守护即主死子死.
                        m.start()

                    ######################## WordCountClient ##############################
                    # json string + bytes are used for data transmission 
                    elif cmd == "wc":
                        dfs_path = request[1]
                        field_name = request[2]
                        host = request[3]
                        # self.wc(dfs_path, field_name, host, sock_fd)
                        t = threading.Thread(target=self.wc, args=(dfs_path, field_name, host, sock_fd))
                        # m = multiprocessing.Process(target=self.wc, args=(dfs_path, field_name, host, sock_fd))
                        t.daemon = True  # daemon True设置为守护即主死子死.
                        t.start()
                    
                    elif cmd == 'status':
                        t = threading.Thread(target=self.status, args=(sock_fd,))
                        t.daemon = True
                        t.start()
                        
                    ########################################################################
                    

                    elif cmd == "checksum":  # 加载数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.checksum(dfs_path)
                    elif cmd == "store":  # 存储数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        t = threading.Thread(target = self.store, args=(sock_fd, dfs_path,))
                        t.daemon = True
                        t.start()
                    elif cmd == "rm":  # 删除数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.rm(dfs_path)
                    elif cmd == "format":  # 格式化DFS
                        response = self.format(sock_fd)
                    elif cmd == "mean":
                        dfs_path = request[1]
                        field_name = request[2]
                        response = self.mean(dfs_path, field_name)
                    elif cmd == "var":
                        dfs_path = request[1]
                        field_name = request[2]
                        response = self.var(dfs_path, field_name)
                    else:
                        response = "Undefined command: " + " ".join(request)
                        print(response)
                    # if type(response) == str:
                    #     sock_fd.send(bytes(response, encoding='utf-8'))
                    # elif response:
                    #     # sock_fd.send(response)
                    #     send_data(sock_fd, response)
                except KeyboardInterrupt:
                    break
                # finally:
                #     sock_fd.close()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(e)
        finally:
            listen_fd.close()
        
        t.join()

    ######################################## Word Count #################################################
    def require_data(self, dfs_path, host):
        print('requiring data')
        sock = socket.socket()
        sock.connect((host, data_node_port))
        sock.send(bytes('load '+dfs_path, encoding='utf-8'))
        res = receive_data(sock)
        print('data received')
        sock.close()
        return str(res, encoding='utf-8')

    @property
    def this_host(self):
        return socket.gethostname()

    def wc(self, dfs_path, field_name, host, sock_fd):
        transmit_time = 0
        data_process_time = 0
        field_name = int(field_name)
        res = {'status':True, 'result':{}, 'error': '', 'transmit_time':'', 'data_process_time':''}
        try:
            if host == self.this_host:
                data = self.load(dfs_path)
            else:
                start_time = time.time()
                data = self.require_data(dfs_path, host)
                transmit_time = time.time()-start_time
            start_time = time.time()
            if not data:
                res['result'] = {}
            elif field_name in word_count:
                data = pd.read_csv(StringIO(data), header=None)
                s = ' '.join(list(data[field_name]))
                res['result'] = count_word_dict(s)
            else:
                print('not applicable!')
            data_process_time = time.time() - start_time
            print("data_process_time == {}".format(data_process_time))
            res['transmit_time'] = str(transmit_time)
            res['data_process_time'] = str(data_process_time)
        except Exception as e:
            res['error'] = str(e)
            res['status'] = False
        send_data(sock_fd, serialize_data(res))
        print('success: {}, data sent'.format(res['status']))
        # print(sock_fd)
        # time.sleep(0.1)
        # sock_fd.close()

    def status(self, sock):
        data = {}
        load = os.popen('uptime | cut -c 41-')
        for line in load.readlines():
            load = line.split(':')[1].split(',')[0]
            data['load'] = float(load)
        send_data(sock, serialize_data(data))

    #######################################################################################################


    def heartbeat(self):
        msg = 'heartbeat' + ' ' + socket.gethostname()
        while True:
            try:
                name_node_sock = socket.socket()
                name_node_sock.connect((name_node_host, heartbeat_port))
                name_node_sock.send(bytes(msg, encoding='utf-8'))
                time.sleep(heartbeat_interval)
            except KeyboardInterrupt:
                pass
            except Exception as e:
                print(e)
            finally:
                name_node_sock.close()
    
    def mean(self, dfs_path, field_name):
        def word_count_f(s):
            if not s:
                return 0
            return len(s.split())

        def cal_rate(s):
            if not s:
                return 0
            s = eval(s)
            s = [ss.replace(',', '') for ss in s]
            if int(s[1]) == 0:
                return 0
            return int(s[0])*1.0 / int(s[1])

        data = self.load(dfs_path)
        if not data:
            return pickle.dumps([0,0])
        data = pd.read_csv(StringIO(data), header=None)
        field_name = int(field_name)
        if field_name in not_applicable:
            return None
        elif field_name in number:
            res = [np.sum(list(data[field_name])), len(data)]
        elif field_name in word_count:
            # print('word count!')
            word_cnt = list(data[field_name].map(word_count_f))
            res = [np.sum(word_cnt), len(word_cnt)]
            # print(res)
        elif field_name in rate:
            rate_list = list(data[field_name].map(cal_rate))
            res = [np.sum(rate_list), len(rate_list)]
        else:
            print('error, invalid field name {}'.format(field_name))
            res = None
        return pickle.dumps(res)


    def var(self, dfs_path, field_name):
        def word_count_f(s):
            if not s:
                return 0
            return len(s.split())
        def cal_rate(s):
            if not s:
                return 0
            s = eval(s)
            s = [ss.replace(',', '') for ss in s]
            if int(s[1]) == 0:
                return 0
            return int(s[0])*1.0 / int(s[1])

        data = self.load(dfs_path)
        if not data:
            return pickle.dumps([0,0,0])
        data = pd.read_csv(StringIO(data), header=None)
        field_name = int(field_name)
        if field_name in not_applicable:
            return None
        elif field_name in number:
            res = [np.nansum(np.square(list(data[field_name]))), np.nansum(list(data[field_name])), len(data[field_name][~np.isnan(data[field_name])])]
        elif field_name in word_count:
            word_cnt = list(data[field_name].map(word_count_f))
            res = [np.sum(np.square(word_cnt)), np.sum(word_cnt), len(word_cnt)]
        elif field_name in rate:
            rate_list = list(data[field_name].map(cal_rate))
            res = [np.sum(np.square(rate_list)), np.sum(rate_list), len(rate_list)]
        else:
            print('error, invalid field name {}'.format(field_name))
            res = None
        return pickle.dumps(res)

    def load_multi(self, dfs_path, sock_fd):
        data = self.load(dfs_path)
        print('start sending data')
        sent = send_data(sock_fd, bytes(data, encoding='utf-8'))
        if sent:
            print('required data sent')
        else:
            print('error when sending data')
        # sock_fd.close()


    def load(self, dfs_path):
        # 本地路径
        local_path = os.path.join(data_node_dir, dfs_path)
        # 读取本地数据
        with open(local_path) as f:
            chunk_data = f.read()
        return chunk_data
        # send_data(sock_fd, chunk_data)
    
    def checksum(self, dfs_path):
        # 本地路径
        local_path = os.path.join(data_node_dir, dfs_path)
        if not os.path.exists(local_path):
            return "0"
        # 读取本地数据
        with open(local_path) as f:
            chunk_data = f.read()
        
        checksum_save_path = os.path.join(checksum_node_dir, dfs_path)
        f = open(checksum_save_path)
        checksum = f.readlines()[0].strip()
        f.close()
        if hashlib.md5(bytes(chunk_data, encoding='utf-8')).hexdigest() == checksum:
            return "1"
        else:
            return "0"
    
    def store(self, sock_fd, dfs_path):
        # 从Client获取块数据
        sock_fd.send(bytes('ready', encoding='utf-8'))
        chunk_data = b''
        while True:
            part = sock_fd.recv(BUF_SIZE * 2)
            chunk_data += part
            if len(part) == 0:
                # either 0 or end of data
                break
        print(len(chunk_data))
        # chunk_data = sock_fd.recv(BUF_SIZE*2)
        # 本地路径
        local_path = os.path.join(data_node_dir, dfs_path)
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 将数据块写入本地文件
        with open(local_path, "wb") as f:
            f.write(chunk_data)

        checksum_save_path = os.path.join(checksum_node_dir, dfs_path)
        os.system("mkdir -p {}".format(os.path.dirname(checksum_save_path)))
        with open(checksum_save_path, 'w')as f:
            f.writelines(hashlib.md5(chunk_data).hexdigest())

        # return "Store chunk {} successfully~".format(local_path)
    
    def rm(self, dfs_path):
        local_path = os.path.join(data_node_dir, dfs_path)
        rm_command = "rm -rf " + local_path
        os.system(rm_command)
        
        return "Remove chunk {} successfully~".format(local_path)
    
    def format(self, sock):
        format_command = "rm -rf {}/*".format(data_node_dir)
        os.system(format_command)
        
        send_data(sock, bytes("Format datanode successfully~", encoding='utf-8'))


# 创建DataNode对象并启动
data_node = DataNode()
data_node.run()