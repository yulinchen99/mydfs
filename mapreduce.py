# review_id,reviewer,movie,rating,review_summary,review_date,spoiler_tag,review_detail,helpful
'''
this mapreduce client calculates mean and variance for multiple data field for 
0. review_id: not applicable
1. reviewer: not applicable
2. movie: word count
3. rating: number
4. review_summary: word count
5. review_date: not applicable
6. spoiler_tag: 0/1
7. review_detail: word count
8. helpful: float (list[0]/list[1])
'''

import os
import socket
import time
from io import StringIO

import pandas as pd

from common import *
import json
from client import Client
import pickle # use pickle dumps and loads to transform a data structure to bytes
import math
import random
import multiprocessing

not_applicable = [0, 1, 5]


class MapReduceClient(Client):
    def __init__(self):
        self.name_node_sock = socket.socket()
        self.name_node_sock.connect((name_node_host, name_node_port))
    
    def __del__(self):
        self.name_node_sock.close()
    
    def mean(self, dfs_path, field_name):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # TODO: 从NameNode获取一张FAT表；打印FAT表；根据FAT表逐个从目标DataNode请求数据块，写入到本地文件中
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        # blk_no, host_name, blk_size
        total_sum = 0.0
        total_cnt = 0
        blk_nos = sorted(list(set(fat['blk_no'])))
        for blk_no in blk_nos:
            blk_data = self.get_blk_mean(fat, blk_no, dfs_path, field_name)
            if blk_data is None:
                print("no correct replica found for blk_no {}".format(blk_no))
                break
            else:
                total_sum += blk_data[0]
                total_cnt += blk_data[1]
        return total_sum * 1.0 / total_cnt
    
    def var(self, dfs_path, field_name):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # TODO: 从NameNode获取一张FAT表；打印FAT表；根据FAT表逐个从目标DataNode请求数据块，写入到本地文件中
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        # blk_no, host_name, blk_size
        total_square_sum = 0.0
        total_sum = 0.0
        total_cnt = 0
        blk_nos = sorted(list(set(fat['blk_no'])))
        for blk_no in blk_nos:
            blk_data = self.get_blk_var(fat, blk_no, dfs_path, field_name)
            if blk_data is None:
                print("no correct replica found for blk_no {}".format(blk_no))
                break
            else:
                total_square_sum += blk_data[0]
                total_sum += blk_data[1]
                total_cnt += blk_data[2]
        return total_square_sum*1.0 / total_cnt - (total_sum*1.0 / total_cnt)**2

    def load_blk_var(self, host, blk_path, field_name):
        data_node_sock = socket.socket()
        data_node_sock.connect((host, data_node_port))
        request = "var {} {}".format(blk_path, field_name)
        data_node_sock.send(bytes(request, encoding='utf-8'))
        blk_data = data_node_sock.recv(BUF_SIZE)
        data_node_sock.close()
        return pickle.loads(blk_data)


    def load_blk_mean(self, host, blk_path, field_name):
        data_node_sock = socket.socket()
        data_node_sock.connect((host, data_node_port))
        request = "mean {} {}".format(blk_path, field_name)
        data_node_sock.send(bytes(request, encoding='utf-8'))
        blk_data = data_node_sock.recv(BUF_SIZE)
        data_node_sock.close()
        return pickle.loads(blk_data)

    def get_blk_var(self, fat, blk_no, dfs_path, field_name):
        """
        return [square_sum, sum, count] or None
        """
        available_host = list(fat[fat['blk_no']==blk_no]['host_name'])
        error_host = []
        blk_path = dfs_path + ".blk{}".format(blk_no)
        for host in available_host:
            if self.checksum(host, blk_path):
                blk_data = self.load_blk_var(host, blk_path, field_name)
                # data = json.loads(str(blk_data, encoding='utf-8'))
                self.copyFromHostToHost(blk_path, error_host, blk_data)
                return blk_data
            else:
                print('host: {}, invalid checksum for {}'.format(host, blk_path))
                error_host.append(host)
        return None

    
    def get_blk_mean(self, fat, blk_no, dfs_path, field_name):
        """
        return [sum, count] or None
        """
        available_host = list(fat[fat['blk_no']==blk_no]['host_name'])
        error_host = []
        blk_path = dfs_path + ".blk{}".format(blk_no)
        for host in available_host:
            if self.checksum(host, blk_path):
                blk_data = self.load_blk_mean(host, blk_path, field_name)
                # data = json.loads(str(blk_data, encoding='utf-8'))
                self.copyFromHostToHost(blk_path, error_host, blk_data)
                return blk_data
            else:
                print('host: {}, invalid checksum for {}'.format(host, blk_path))
                error_host.append(host)
        return None
    
    def copyFromLocalByLine(self, local_path, dfs_path):

        def send_data(socket, data):
            data = bytes(data, encoding='utf-8')
            sent = 0
            while sent < len(data):
                sent_part = socket.send(data[sent:sent+BUF_SIZE])
                sent += sent_part
                # print('sent size:', sent_part)
            socket.close()
            return

        file_size = os.path.getsize(local_path)
        print("File size: {}".format(file_size))

        request = "new_fat_item {} {}".format(dfs_path, file_size)
        print("Request: {}".format(request))
        
        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        print('request sent')
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 根据FAT表逐个向目标DataNode发送数据块
        fp = open(local_path)
        blk_no = None
        last_line = None
        for idx, row in fat.iterrows():
            if blk_no is None or blk_no != row['blk_no']:
                # data = fp.readlines()
                data = fp.readlines(int(row['blk_size']))
                data = ''.join(data)
                # if last_line:
                #     data = [last_line] + data
                # last_line = data[]
                # data 
            blk_no = row['blk_no']

            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            
            request = "store {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            # time.sleep(1.0)  # 两次传输需要间隔一s段时间，避免粘包
            while True:
                res = data_node_sock.recv(BUF_SIZE)
                if res and str(res, encoding='utf-8') == 'ready':
                    break
            
            send_data(data_node_sock, data)
            # t = multiprocessing.Process(target=send_data, args=(data_node_sock, data, ))
            # t.start()
            
        fp.close()


if __name__ == '__main__':
    # 解析命令行参数并执行对于的命令
    import sys

    argv = sys.argv
    argc = len(argv) - 1

    client = MapReduceClient()
    result = None

    cmd = argv[1]
    if cmd == '-mean':
        if argc == 3:
            dfs_path = argv[2]
            field_name = int(argv[3])
            if field_name in not_applicable:
                print('--mean is not applicable on field_id {}'.format(field_name))
            else:
                result = client.mean(dfs_path, field_name)
        else:
            print("Usage: python client.py -mean <dfs_path> <field_id>")
    elif cmd == "-var":
        if argc == 3:
            dfs_path = argv[2]
            field_name = int(argv[3])
            if field_name in not_applicable:
                print('--var is not applicable on field_id {}'.format(field_name))
            else:
                result = client.var(dfs_path, field_name)
        else:
            print("Usage: python client.py -var <dfs_path> <field_id>")
    elif cmd == "-copyFromLocalByLine":
        if argc == 3:
            local_path = argv[2]
            dfs_path = argv[3]
            client.copyFromLocalByLine(local_path, dfs_path)
        else:
            print("Usage: python client.py -copyFromLocal <local_path> <dfs_path>")
    else:
        print("Undefined command: {}".format(cmd))
        print("Usage: python client.py <-mean | -var> other_arguments")
    if result:
        print(result)