import os
import socket
import time
from io import StringIO

import pandas as pd

from common import *
import json


class Client:
    def __init__(self):
        self.name_node_sock = socket.socket()
        self.name_node_sock.connect((name_node_host, name_node_port))
    
    def __del__(self):
        self.name_node_sock.close()
    
    def ls(self, dfs_path):
        # TODO: 向NameNode发送请求，查看dfs_path下文件或者文件夹信息
        try:
            request = "ls {}".format(dfs_path)
            self.name_node_sock.send(bytes(request, encoding='utf-8'))
            response = self.name_node_sock.recv(BUF_SIZE)
            response = str(response, encoding='utf-8')
            print(response)
        except Exception as e:
            print(e)
    
    def copyFromLocal(self, local_path, dfs_path):
        file_size = os.path.getsize(local_path)
        print("File size: {}".format(file_size))
        
        request = "new_fat_item {} {}".format(dfs_path, file_size)
        print("Request: {}".format(request))
        
        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 根据FAT表逐个向目标DataNode发送数据块
        fp = open(local_path)
        blk_no = None
        for idx, row in fat.iterrows():
            if blk_no is None or blk_no != row['blk_no']:
                data = fp.read(int(row['blk_size']))
            blk_no = row['blk_no']

            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            
            request = "store {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一s段时间，避免粘包
            data_node_sock.sendall(bytes(data, encoding='utf-8'))
            data_node_sock.close()
        fp.close()
    
    def checksum(self, host, blk_path):
        data_node_sock = socket.socket()
        data_node_sock.connect((host, data_node_port))
        request = "checksum {}".format(blk_path)
        data_node_sock.send(bytes(request, encoding='utf-8'))
        blk_data = data_node_sock.recv(BUF_SIZE)
        data_node_sock.close()
        return int(str(blk_data, encoding='utf-8'))

    def load_blk_data(self, host, blk_path):
        """
        return data in bytes
        """
        data_node_sock = socket.socket()
        data_node_sock.connect((host, data_node_port))
        request = "load {}".format(blk_path)
        data_node_sock.send(bytes(request, encoding='utf-8'))
        blk_data = data_node_sock.recv(BUF_SIZE)
        data_node_sock.close()
        return blk_data
    
    def get_blk_data(self, fat, blk_no, dfs_path):
        """
        return data in bytes
        """
        available_host = list(fat[fat['blk_no']==blk_no]['host_name'])
        error_host = []
        blk_path = dfs_path + ".blk{}".format(blk_no)
        for host in available_host:
            if self.checksum(host, blk_path):
                blk_data = self.load_blk_data(host, blk_path)
                # data = json.loads(str(blk_data, encoding='utf-8'))
                self.copyFromHostToHost(blk_path, error_host, blk_data)
                return blk_data
            else:
                print('host: {}, invalid checksum for {}'.format(host, blk_path))
                error_host.append(host)
    
    def copyFromHostToHost(self, blk_path, bad_hosts, data):
        for host in bad_hosts:
            print('fix {} error on {}'.format(host, blk_path))
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))
            request = "store {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一s段时间，避免粘包
            data_node_sock.sendall(data)
            data_node_sock.close()
        
    
    def copyToLocal(self, dfs_path, local_path):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # TODO: 从NameNode获取一张FAT表；打印FAT表；根据FAT表逐个从目标DataNode请求数据块，写入到本地文件中
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        # blk_no, host_name, blk_size
        fp = open(local_path, 'wb')
        blk_nos = sorted(list(set(fat['blk_no'])))
        for blk_no in blk_nos:
            blk_data = self.get_blk_data(fat, blk_no, dfs_path)
            if blk_data is None:
                print("no correct replica found for blk_no {}".format(blk_no))
            else:
                fp.write(blk_data)
        fp.close()

    
    def rm(self, dfs_path):
        request = "rm_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # TODO: 从NameNode获取改文件的FAT表，获取后删除；打印FAT表；根据FAT表逐个告诉目标DataNode删除对应数据块
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        request = "rm {}".format(dfs_path)
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        response = self.name_node_sock.recv(BUF_SIZE)
        print(str(response, 'utf-8'))

        for idx, row in fat.iterrows():            
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            
            request = "rm {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            res = data_node_sock.recv(BUF_SIZE)
            data_node_sock.close()
            print(str(res, 'utf-8'))



    def format(self):
        request = "format"
        print(request)
        
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        print(str(self.name_node_sock.recv(BUF_SIZE), encoding='utf-8'))
        
        for host in host_list:
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))
            
            data_node_sock.send(bytes("format", encoding='utf-8'))
            print(str(data_node_sock.recv(BUF_SIZE), encoding='utf-8'))
            
            data_node_sock.close()

# 解析命令行参数并执行对于的命令
if __name__ =='__main__':
    import sys

    argv = sys.argv
    argc = len(argv) - 1

    client = Client()

    cmd = argv[1]
    if cmd == '-ls':
        if argc == 2:
            dfs_path = argv[2]
            client.ls(dfs_path)
        else:
            print("Usage: python client.py -ls <dfs_path>")
    elif cmd == "-rm":
        if argc == 2:
            dfs_path = argv[2]
            client.rm(dfs_path)
        else:
            print("Usage: python client.py -rm <dfs_path>")
    elif cmd == "-copyFromLocal":
        if argc == 3:
            local_path = argv[2]
            dfs_path = argv[3]
            client.copyFromLocal(local_path, dfs_path)
        else:
            print("Usage: python client.py -copyFromLocal <local_path> <dfs_path>")
    elif cmd == "-copyToLocal":
        if argc == 3:
            dfs_path = argv[2]
            local_path = argv[3]
            client.copyToLocal(dfs_path, local_path)
        else:
            print("Usage: python client.py -copyFromLocal <dfs_path> <local_path>")
    elif cmd == "-format":
        client.format()
    else:
        print("Undefined command: {}".format(cmd))
        print("Usage: python client.py <-ls | -copyFromLocal | -copyToLocal | -rm | -format> other_arguments")
