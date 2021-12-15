from abc import abstractclassmethod, abstractmethod
from common import *
from typing import *
from util.asset import receive_data
from util.job import *

import math
import os
import socket

import numpy as np
import pandas as pd

import time
import threading
import random
from collections import defaultdict


class SchedulerBase:
    # the base class for scheduler
    def __init__(self):
        self.task_pool = []
        self.job_pool = []
        self.task2port = {}
        self.task2job = {}
        self.job2transmissioncost = defaultdict(float) # data transmission between datanodes cost
        self.job2schedulecost = defaultdict(float) # time cost on scheduling
        self.job2processcost = defaultdict(float) # actual process time on data node
        self.datanode_load = {host:0 for host in host_list}
        self.datanode_delay = {host:0 for host in host_list}
        self.datanode_port = data_node_port
        self.max_load = max_load
        self._pushing = False

    def listen_for_client(self):
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", scheduler_port))
            listen_fd.listen(5)
            print('scheduler listening on', scheduler_port )
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                try:
                    # 获取请求方发送的指令
                    request = str(sock_fd.recv(128), encoding='utf-8')
                    request = request.split(' ')  # 指令之间使用空白符分割
                    
                    cmd = request[0]  # 指令第一个为指令类型
                    response = None
                    if cmd == 'submit_job':
                        response = self.submit_job(sock_fd)

                except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
                    pass
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
    
    @property
    def free_data_node(self):
        return [host for host in self.datanode_load if self.datanode_load[host] < self.max_load]

    def _run_task(self, host):
        while True:
            if not self._pushing:
                break
        task = self.find_next_task(host)
        if task:
            port = task.port
            # port = self.task2port[task]
            sock = create_sock(main_host, port)
            sock.send(bytes('{} {}'.format(task.task_id, host), encoding='utf-8'))

            data = receive_data(sock)
            data = deserialize_data(data)
            sock.close()
            "TODO record cost"
            if not data:
                self.task_pool.append(task)
        self.datanode_load[host] -= 1

    
    def _run(self):
        while True:
            free_host = self.free_data_node
            if self.task_pool and free_host:
                self.infer(free_host)
                # threads = []
                for host in free_host:
                    if self.task_pool:
                        self.datanode_load[host] += 1
                        # self._run_task(host)
                        # print('one task completed')
                        # print('task left:', len(self.task_pool))
                        # print('free host:', len(self.free_data_node))
                        t = threading.Thread(target = self._run_task, args=(host,))
                        # threads.append(t)
                        t.start()
                        # time.sleep(0.1)
                # for t in threads:
                #     t.join()

    def run(self):
        threads = []
        t = threading.Thread(target=self._run)
        threads.append(t)

        t = threading.Thread(target=self.listen_for_client)
        threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

                    
    
    @abstractmethod
    def find_next_task(self, host):
        raise NotImplementedError
    
    def infer(self, free_host):
        pass
                

    def push_task(self, tasks: List[Task], job):
        self._pushing = True
        # add task to pool
        self.task_pool += tasks
        # record task2port and task2jobhash and jobhashlist
        self.job_pool.append(job)
        for task in tasks:
        #     self.task2port[task] = job.port 
            self.task2job[task] = hash(job)
        self._pushing = False

    def submit_job(self, sock_fd):
        print('submit job request')
        sock_fd.send(bytes('ready', encoding='utf-8'))
        print('receiving jobs')
        data = receive_data(sock_fd)
        job = pickle.loads(data)
        tasks = list(job.tasks.values())
        print('pushing jobs')
        self.push_task(tasks, job)
        return None


class RandomScheduler(SchedulerBase):
    def find_next_task(self, host):
        task = random.choice(self.task_pool)
        self.task_pool.remove(task)
        return task
    
    # def find_next_task(self, host):
    #     for task in self.task_pool:
    #         if host not in task.preferred_datanode:
    #             self.task_pool.remove(task)
    #             return task

class DataLocalityScheduler(SchedulerBase):
    def find_next_task(self, host):
        for task in self.task_pool:
            if host in task.preferred_datanode:
                self.task_pool.remove(task)
                return task

if __name__ == '__main__':
    scheduler = DataLocalityScheduler()
    # print('using random scheduler')
    # scheduler = RandomScheduler()
    scheduler.run()

"""
TODO
QuincyScheduler
"""
