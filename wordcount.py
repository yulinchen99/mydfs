# %%
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
from util.job import JobRunner, WordCountJob, Task

applicable = [2, 4, 7]


class WordCountClient(Client):
    def __init__(self):
        self.name_node_sock = socket.socket()
        self.name_node_sock.connect((name_node_host, name_node_port))
    
    def __del__(self):
        self.name_node_sock.close()
    
    def wordcount(self, dfs_path, field_name):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # TODO: 从NameNode获取一张FAT表；打印FAT表；根据FAT表逐个从目标DataNode请求数据块，写入到本地文件中
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        job = WordCountJob(0, fat, dfs_path, field_name=field_name)
        jobrunner = JobRunner(job)
        result = jobrunner.run()
        print('job done. result received')
        # print(result)
        return result
# %%

if __name__ == '__main__':
    # 解析命令行参数并执行对于的命令
    import sys

    argv = sys.argv
    argc = len(argv) - 1

    client = WordCountClient()
    result = None

    cmd = argv[1]
    if cmd == '-wc':
        if argc == 3:
            dfs_path = argv[2]
            field_name = int(argv[3])
            if field_name not in applicable:
                print('--wc is not applicable on field_id {}'.format(field_name))
            else:
                result = client.wordcount(dfs_path, field_name)
        else:
            print("Usage: python client.py -wc <dfs_path> <field_id>")
    else:
        print("Undefined command: {}".format(cmd))
        print("Usage: python client.py -wc other_arguments")
    if result:
        # print(result)
        with open('wc-result.json', 'w')as f:
            f.writelines(json.dumps(result, ensure_ascii=False))
        print('result saved')