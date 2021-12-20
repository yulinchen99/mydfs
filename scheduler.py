from abc import abstractclassmethod, abstractmethod
from common import *
from typing import *
from util.asset import receive_data
from util.job import *
from util.mincostflow import MinCostFlow

import math
import os
import socket

import numpy as np
import pandas as pd

import time
import threading
import random
from collections import defaultdict
import multiprocessing


class SchedulerBase:
    # the base class for scheduler
    def __init__(self):
        # self.task_pool = []
        self.job_pool = []
        self.task2port = {}
        self.task2job = {}
        self.task2status = {}
        self.job2transmissioncost = defaultdict(float) # data transmission between datanodes cost
        self.job2schedulecost = defaultdict(float) # time cost on scheduling
        self.job2processcost = defaultdict(float) # actual process time on data node
        self.datanode_load = {host:0 for host in host_list}
        self.datanode_delay = {host:0 for host in host_list}
        self.datanode_port = data_node_port
        self.max_load = max_load
        self._pushing = False
    
    @property
    def task_pool(self):
        temp_dict = self.task2status.copy()
        return [task for task in temp_dict if not temp_dict[task]]


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

    def _run_task(self, host, infer_time):
        while True:
            if not self._pushing:
                break
        start_time = time.time()
        task = self.find_next_task(host)
        self.task2status[task] = True
        scheduler_time = time.time()-start_time + infer_time
        if task:
            port = task.port
            # port = self.task2port[task]
            sock = create_sock(main_host, port)
            sock.send(bytes('{} {} {}'.format(task.task_id, host, str(scheduler_time)), encoding='utf-8'))
            data = sock.recv(BUF_SIZE * 2)
            data = deserialize_data(data)
            sock.close()
            "TODO record cost"
            if not data:
                print('task failed')
                self.task2status[task] = False

        self.datanode_load[host] -= 1
        # print('task left:', len(self.task_pool))
        # print('free host:', len(self.free_data_node))
        return

    
    def _run(self):
        while True:
            free_host = self.free_data_node
            if self.task_pool and free_host:
                start_time = time.time()
                # self.infer(free_host)
                infer_time = time.time()-start_time
                # threads = []
                for host in free_host:
                    if self.task_pool:
                        self.datanode_load[host] += 1
                        # self._run_task(host)
                        # print('one task completed')
                        m = threading.Thread(target=self._run_task, args=(host, infer_time))
                        m.daemon = True
                        m.start()
                        
                        # t = threading.Thread(target = self._run_task, args=(host,))
                        # threads.append(t)
                        # t.start()
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
        # self.task_pool += tasks
        # record task2port and task2jobhash and jobhashlist
        self.job_pool.append(job)
        for task in tasks:
        #     self.task2port[task] = job.port 
            self.task2job[task] = hash(job)
            self.task2status[task] = False
        self._pushing = False

    def submit_job(self, sock_fd):
        # print('submit job request')
        sock_fd.send(bytes('ready', encoding='utf-8'))
        # print('receiving jobs')
        data = receive_data(sock_fd)
        job = pickle.loads(data)
        tasks = list(job.tasks.values())
        # print('pushing jobs')
        self.push_task(tasks, job)
        return None


class RandomScheduler(SchedulerBase):
    def find_next_task(self, host):
        task = random.choice(self.task_pool)
        return task
    
    # def find_next_task(self, host):
    #     for task in self.task_pool:
    #         if host not in task.preferred_datanode:
    #             self.task_pool.remove(task)
    #             return task

class DataLocalityScheduler(SchedulerBase):
    def push_task(self, tasks: List[Task], job):
        self._pushing = True
        # add task to pool
        # self.task_pool += tasks
        # record task2port and task2jobhash and jobhashlist
        self.job_pool.append(job)
        for task in tasks:
        #     self.task2port[task] = job.port 
            self.task2job[task] = hash(job)
            self.task2status[task] = False
            # self.node2tasks = {}
        self._pushing = False

    def find_next_task(self, host):
        for task in self.task_pool:
            if host in task.preferred_datanode:
                return task

class QuincyScheduler(SchedulerBase):
    # TODO:三部分cost的计算
    # cost_process:任务耗时，为wc数据集的size乘host的拥塞程度
    # cost_transmission：数据传输耗时，若数据不在该host如何计算
    # self.transmissioncost[host]：通信耗时，Scheduler与host的通信耗时

    def __init__(self, alpha: float, beta: float, gamma: float):
        super(QuincyScheduler, self).__init__()
        self.mincostflow = MinCostFlow()
        self.alpha = float(alpha)
        self.beta = float(beta)
        self.gamma = float(gamma)
        self.transmissioncost = {}
        self.load = {}
        for host in self.datanode_load:
            self.transmissioncost[host] = 1.0
            self.load[host] = 1.0

    def cal_cost(self, task, host):
        cost_process = task.size*self.load[host]
        if host in task.preferred_datanode:
            cost_transmission = 0.0
        else:
            cost_transmission = 1.0
        return self.alpha*cost_process + self.beta*cost_transmission + self.gamma*self.transmissioncost[host]

    def infer(self, free_host):
        hosts = [host for host in self.datanode_load]
        self.mincostflow.init()
        self.mincostflow.set_free_host(hosts)
        tasks = self.task_pool
        self.mincostflow.set_tasks(tasks)
        len_tasks = len(tasks)
        len_hosts = len(hosts)
        self.mincostflow.set_supply(len_tasks)
        for i in range(len_tasks):
            self.mincostflow.add_edge(0, i+1, 1, 0)
            self.mincostflow.set_supply(0)
        for i in range(len_hosts):
            self.mincostflow.set_supply(0)
            self.mincostflow.add_edge(i+len_tasks+1, len_hosts+len_tasks+1, 999, 0)
            if hosts[i] == free_host:
                self.mincostflow.add_edge(i + len_tasks + 1, len_hosts + len_tasks + 1, 1, -9999999999)
        self.mincostflow.set_supply(-1*len_tasks)
        for i in range(len_tasks):
            for j in range(len_hosts):
                self.mincostflow.add_edge(i+1, j+len_tasks+1, 1, int(self.cal_cost(tasks[i], hosts[j])))
        self.mincostflow.infer()
        # self.mincostflow.print()

    def find_next_task(self, host):
        # print(self.mincostflow.find_next_task(host).task_cmd)
        self.infer(host)
        return self.mincostflow.find_next_task(host)
        # for task in self.task_pool:
        #     if host in task.preferred_datanode:
        #         return task

if __name__ == '__main__':
    import sys

    argv = sys.argv
    argc = len(argv) - 1
    cmd = argv[1]
    if cmd == 'Quincy':
        if argc ==4:
            scheduler = QuincyScheduler(argv[2], argv[3], argv[4])
        else:
            print("Usage: python client.py -Quincy alpha beta gamma")
    elif cmd == 'Random':
        scheduler = RandomScheduler()
    elif cmd == 'DataLocality':
        scheduler = DataLocalityScheduler()
    else:
        print("Undefined command: {}".format(cmd))
    scheduler.run()