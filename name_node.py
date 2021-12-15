import math
import os
import socket

import numpy as np
import pandas as pd

from common import *
import time
import threading
from util.asset import *

# NameNode功能
# 1. 保存文件的块存放位置信息
# 2. ls ： 获取文件/目录信息
# 3. get_fat_item： 获取文件的FAT表项
# 4. new_fat_item： 根据文件大小创建FAT表项
# 5. rm_fat_item： 删除一个FAT表项
# 6. format: 删除所有FAT表项

class NameNode:
    def run(self):  # 启动NameNode
        self.init()
        threads = []
        t = threading.Thread(target=self.check_host_status_loop)
        threads.append(t)
        t = threading.Thread(target=self.listen_for_heartbeat)
        threads.append(t)
        t = threading.Thread(target=self.main_loop)
        threads.append(t)
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()

    def main_loop(self):
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", name_node_port))
            listen_fd.listen(5)
            print("Name node started")
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                
                try:
                    # 获取请求方发送的指令
                    # request = receive_data(sock_fd)
                    # request = str(request, encoding='utf-8')
                    request = str(sock_fd.recv(128), encoding='utf-8')
                    request = request.split(' ')  # 指令之间使用空白符分割
                    
                    cmd = request[0]  # 指令第一个为指令类型
                    response = None
                    if cmd == 'heartbeat':
                        # print('receive heartbeat from {}'.format(request[1]))
                        response = self.receive_heartbeat(request[1])
                    else:
                        print("connected by {}".format(addr))
                        print("Request: {}".format(request))
                        if not self.initial_check:
                            response = "still initializing...please wait for a few seconds"
                        else:
                            while self.on_check_host_status: # safety strategy
                                time.sleep(0.1)

                            if cmd == "ls":  # 若指令类型为ls, 则返回DFS上对于文件、文件夹的内容
                                dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                                response = self.ls(dfs_path)
                            elif cmd == "get_fat_item":  # 指令类型为获取FAT表项
                                dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                                response = self.get_fat_item(dfs_path)
                            elif cmd == "new_fat_item":  # 指令类型为新建FAT表项
                                dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                                file_size = int(request[2])
                                response = self.new_fat_item(dfs_path, file_size)
                            elif cmd == "update_fat_item":  # 指令类型为更新FAT表项
                                dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                                # fat = request[2]
                                self.update_fat_item(dfs_path, sock_fd)
                            elif cmd == "rm_fat_item":  # 指令类型为删除FAT表项
                                dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                                response = self.rm_fat_item(dfs_path)
                            elif cmd == "format":
                                response = self.format()                            
                            else:  # 其他位置指令
                                response = "Undefined command: " + " ".join(request)

                    if response is not None:
                        print("Response: {}".format(response))
                        sock_fd.send(bytes(response, encoding='utf-8'))
                except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
                    break
                except Exception as e:  # 如果出错则打印错误信息
                    print(e)
                finally:
                    sock_fd.close()  # 释放连接
        except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
            pass
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            listen_fd.close()  # 释放连接
        
    
    def init(self):
        "initialize necessary data for heartbeat and host status"
        self.host_status_dict = {host:False for host in host_list}
        self.heartbeat_dict = {host:None for host in host_list}
        self.initial_check = False # whether the first check has been performed
    
    # ############################# heartbeat ###################################
    def receive_heartbeat(self, hostname):
        self.heartbeat_dict[hostname] = time.time()
        self.host_status_dict[hostname] = True # the host will be set alive once heartbeat is recieved
        # return 'get heartbeat from {}'.format(hostname)

    def check_host_status(self):
        "main function to check host status, only detection of down data node is performed (only switch status to False)"
        # print('perform check')
        period = heartbeat_interval
        new_down_host_list = [] # the newly detected down host in this specific check
        self.on_check_host_status = True # safety insurance
        for host in host_list:
            if self.heartbeat_dict[host] is None or time.time() - self.heartbeat_dict[host] > period * 1.5: # time interval too long
                if self.host_status_dict[host]: # newly detected
                    new_down_host_list.append(host)
                    print("[WARNING] data node {} is down".format(host))
                    self.host_status_dict[host] = False
        self.launch_replicate(new_down_host_list) # handle down data node
        self.on_check_host_status = False
    
    def listen_for_heartbeat(self):
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", heartbeat_port))
            listen_fd.listen(5)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                
                try:
                    # 获取请求方发送的指令
                    request = str(sock_fd.recv(128), encoding='utf-8')
                    request = request.split()  # 指令之间使用空白符分割
                    
                    cmd = request[0]  # 指令第一个为指令类型
                    response = None
                    if cmd == 'heartbeat':
                        # print('receive heartbeat from {}'.format(request[1]))
                        response = self.receive_heartbeat(request[1])
                    else:
                        print("unrecognized message")
                    if response is not None:
                        print("Response: {}".format(response))
                        sock_fd.send(bytes(response, encoding='utf-8'))
                except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
                    break
                except Exception as e:  # 如果出错则打印错误信息
                    print(e)
                finally:
                    sock_fd.close()  # 释放连接
        except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
            pass
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            listen_fd.close()  # 释放连接
    
    def check_host_status_loop(self):
        time.sleep(0.1)
        self.check_host_status() # immediately check when lauching
        self.initial_check = True
        period = heartbeat_interval
        while True:
            time.sleep(period*1.5) # check periodically
            self.check_host_status()
    # ####################################################################################

    # ######################## data replication for falsified node ##########################
    @property
    def all_fat_files(self):
        allfiles = []
        def getallfiles(root, path):
            childFiles= os.listdir(os.path.join(root, path))
            for file in childFiles:
                filepath = os.path.join(root, path, file)
                if os.path.isdir(filepath):
                    getallfiles(root, os.path.join(path, file))
                else:
                    allfiles.append(os.path.join(path, file))
        getallfiles(name_node_dir, '.')
        return allfiles

    def copy_from_host_to_host(self, source_host, target_host, blk_path):
        print('copy {} from {} to {}'.format(blk_path, source_host, target_host))
        # load data from source
        source_data_node_sock = socket.socket()
        source_data_node_sock.connect((source_host, data_node_port))
        
        request = "load {}".format(blk_path)
        source_data_node_sock.send(bytes(request, encoding='utf-8'))
        blk_data = b''
        while True:
            part = source_data_node_sock.recv(BUF_SIZE)
            blk_data += part
            if len(part) < BUF_SIZE:
                break
        source_data_node_sock.close()
        # TODO

        # write data to target
        target_data_node_sock = socket.socket()
        target_data_node_sock.connect((target_host, data_node_port))
        request = "store {}".format(blk_path)
        target_data_node_sock.send(bytes(request, encoding='utf-8'))
        time.sleep(0.2)  # 两次传输需要间隔一s段时间，避免粘包
        target_data_node_sock.send(blk_data)
        target_data_node_sock.close()

    @property
    def alive_node(self):
        return [host for host in self.host_status_dict if self.host_status_dict[host]]
    
    def launch_replicate(self, hostname_list):
        "the main method to perform data replication for data on down data node"
        for filepath in self.all_fat_files:
            df = pd.read_csv(os.path.join(name_node_dir, filepath))
            df_to_replicate = df[df['host_name'].isin(hostname_list)] # get data blk to replicate
            df_old = df[~df['host_name'].isin(hostname_list)] # other parts
            if not len(df_to_replicate): # no data replication needed
                continue

            new_host_list = []
            for i, row in df_to_replicate.iterrows():
                blk_no = row['blk_no']
                blk_path = filepath + ".blk{}".format(blk_no)

                source_host_candidate = list(df_old[df_old['blk_no']==blk_no]['host_name']) # has the data blk while not down
                if not source_host_candidate:
                    print("[ERROR] no available source node, all nodes are down")
                    new_host_list.append(row['host_name'])
                    continue

                source_host = source_host_candidate[0]  # use the first node
                
                target_host_candidate = list(set(self.alive_node).difference(set(source_host_candidate))) # do not store same data twice on same data node
                if not target_host_candidate:
                    print('[ERROR]: no available target node')
                    new_host_list.append(row['host_name'])
                    continue
                target_host = target_host_candidate[0] # use first one, better strategy can be designed

                self.copy_from_host_to_host(source_host, target_host, blk_path)

                # update host
                new_host_list.append(target_host)
            
            # update fat
            df_to_replicate['host_name'] = new_host_list
            df_new = pd.concat([df_to_replicate, df_old], axis=0)
            local_path = os.path.join(name_node_dir, filepath)
            df_new.to_csv(local_path, index=False)
    # #################################################################################

    
    def ls(self, dfs_path):
        local_path = os.path.join(name_node_dir, dfs_path)
        # 如果文件不存在，返回错误信息
        if not os.path.exists(local_path):
            return "No such file or directory: {}".format(dfs_path)
        
        if os.path.isdir(local_path):
            # 如果目标地址是一个文件夹，则显示该文件夹下内容
            dirs = os.listdir(local_path)
            response = " ".join(dirs)
        else:
            # 如果目标是文件则显示文件的FAT表信息
            with open(local_path) as f:
                response = f.read()
        
        return response

    def update_fat_item(self, dfs_path, sock_fd):
        # sock_fd.send(bytes('ready', encoding='utf-8'))
        sock_fd.send(bytes('ready', encoding='utf-8'))
        # time.sleep(0.1)
        data = receive_data(sock_fd)
        print('fat recieved')
        fat = str(data, encoding='utf-8')
        print(fat)
        with open(os.path.join(name_node_dir, dfs_path), 'w')as f:
            f.writelines(fat)
        return None
    
    def get_fat_item(self, dfs_path):
        # 获取FAT表内容
        local_path = os.path.join(name_node_dir, dfs_path)
        response = pd.read_csv(local_path)
        return response.to_csv(index=False)
    
    def new_fat_item_byline(self, dfs_path, file_size, num_lines):
        nb_blks = int(math.ceil(file_size / dfs_blk_size))
        num_line = int(math.ceil(num_lines / nb_blks))
        
        data_pd = pd.DataFrame(columns=['blk_no', 'host_name', 'blk_size'])
        
        rowid = 0
        for i in range(nb_blks):
            blk_no = i
            host_names = np.random.choice(self.alive_node, size=dfs_replication, replace=False)
            blk_size = min(num_line, num_lines - i * num_line)
            for host_name in host_names:
                data_pd.loc[rowid] = [blk_no, host_name, blk_size]
                rowid += 1
        
        # 获取本地路径
        local_path = os.path.join(name_node_dir, dfs_path)
        print(local_path)
        
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 保存FAT表为CSV文件
        data_pd.to_csv(local_path, index=False)
        # 同时返回CSV内容到请求节点
        return data_pd.to_csv(index=False)
    
    def new_fat_item(self, dfs_path, file_size):
        nb_blks = int(math.ceil(file_size / dfs_blk_size))
        print(file_size, nb_blks)
        
        # todo 如果dfs_replication为复数时可以新增host_name的数目
        data_pd = pd.DataFrame(columns=['blk_no', 'host_name', 'blk_size'])
        
        rowid = 0
        for i in range(nb_blks):
            blk_no = i
            host_names = np.random.choice(self.alive_node, size=dfs_replication, replace=False)
            blk_size = min(dfs_blk_size, file_size - i * dfs_blk_size)
            for host_name in host_names:
                data_pd.loc[rowid] = [blk_no, host_name, blk_size]
                rowid += 1
        
        # 获取本地路径
        local_path = os.path.join(name_node_dir, dfs_path)
        print(local_path)
        
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 保存FAT表为CSV文件
        data_pd.to_csv(local_path, index=False)
        # 同时返回CSV内容到请求节点
        return data_pd.to_csv(index=False)
    
    def rm_fat_item(self, dfs_path):
        local_path = name_node_dir + dfs_path
        response = pd.read_csv(local_path)
        os.remove(local_path)
        return response.to_csv(index=False)
    
    def format(self):
        format_command = "rm -rf {}/*".format(name_node_dir)
        os.system(format_command)
        return "Format namenode successfully~"


# 创建NameNode并启动
name_node = NameNode()
name_node.run()
# # %%
# import os
# f = '../data/join/test.txt'
# os.path.split(f)
# # %%
# import pandas as pd
# df = pd.DataFrame({'a':[1,2,3], 'b':[4,5,6]})
# print(df)

# # %%
# df.iloc[1]['a']
# # %%
# class Dum:
#     @property
#     def prop(self):
#         return 1
# # %%
# d = Dum()
# d.prop
# # %%
# pd.concat([df, df], axis=0)
# # %%
# def get_all_fat_files():
#     allfiles = []
#     def getallfiles(root, path):
#         childFiles= os.listdir(os.path.join(root, path))
#         for file in childFiles:
#             filepath = os.path.join(root, path, file)
#             if os.path.isdir(filepath):
#                 getallfiles(root, os.path.join(path, file))
#             else:
#                 allfiles.append(os.path.join(path, file))
#     getallfiles('./dfs/name', '.')
#     return allfiles
# # %%
# get_all_fat_files()
# # %%
