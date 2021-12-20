# %%
from abc import abstractmethod
from collections import defaultdict
from .asset import create_sock, deserialize_data, get_free_port, receive_data, send_data, serialize_data
import socket
import sys
sys.path.append('../')
from common import *
import time
import threading
import pickle
import multiprocessing

class Task:
    def __init__(self, preferred_datanode, cmd, port, task_id, data_path, field_name = None, priority = 0, blk_size = 1.0):
        self.priority = priority
        self.preferred_datanode = preferred_datanode
        self.cmd = cmd
        self.data_path = data_path
        self.completed = False
        self._result = None
        self.timestamp = time.time()
        self.port = port
        self.task_id = task_id
        self._task_cmd = None
        self.field_name = field_name
        self.blk_size = blk_size

    @property
    def task_cmd(self):
        if not self._task_cmd:
            self._task_cmd = ' '.join([self.cmd, self.data_path, str(self.field_name)])
        return self._task_cmd

    @property
    def size(self):
        return self.blk_size

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, newres):
        if self._result is not None:
            print('more than one excecution for task {}'.format(self.task_id))
        self._result = newres

    # def __str__(self):
    #     for attr in self.__getattribute__()
    

class JobBase:
    r"""
    This is only applicable to Jobs that involve one map stage + one reduce stage
    """
    def __init__(self, jobid, fat, data_path, field_name = None):
        self.jobid = jobid
        self.fat = fat
        self.completed_cnt = 0
        self.port = None
        self.data_path = data_path
        self.field_name = field_name
    
    @property
    def completed(self):
        return self.completed_cnt == len(self.tasks)

    def populate_tasks(self):
        self.tasks = self._populate_tasks()
    
    @abstractmethod
    def _populate_tasks(self):
        raise NotImplementedError

    def task_complete(self, task_id):
        if not self.tasks[task_id].completed:
            self.tasks[task_id].completed = True
            self.completed_cnt += 1
        print('current completed task number:{}, remained task number: {}'.format(self.completed_cnt, len(self.tasks)-self.completed_cnt))
    
    def task_result(self, task_id, result):
        self.tasks[task_id].result = result
    
    def get_task_cmd(self, task_id):
        return self.tasks[task_id].task_cmd

    @abstractmethod
    def reduce(self):
        raise NotImplementedError

class WordCountJob(JobBase):
    def _populate_tasks(self):
        tasks = {}
        blk_nos = sorted(list(set(self.fat['blk_no'])))
        for i, blk_no in enumerate(blk_nos):
            if list(self.fat[self.fat['blk_no'] == blk_no]['blk_size'])[0]: # skip empty data
                preferred_node = list(self.fat[self.fat['blk_no'] == blk_no]['host_name'])
                task = Task(preferred_node, 'wc', self.port, i, self.data_path + '.blk{}'.format(blk_no), field_name=self.field_name)
                tasks[i] = task
        return tasks

    def reduce(self):
        print('start reducing')
        wc_res = defaultdict(int)
        for i in self.tasks:
            res = self.tasks[i].result
            for w in res:
                wc_res[w] += res[w]
        return wc_res

    
class JobRunner:
    def __init__(self, job: JobBase):
        self.job = job
        self.completed_cnt = 0
        # 4 metrics
        self.begin_time = time.time()
        self.data_process_time_sum_dict = {host:0 for host in host_list}
        self.transmit_time_sum_dcit = {host:0 for host in host_list}
        self.scheduler_time_sum = 0
    
    def listen_for_scheduler_info(self):
        listen_fd = socket.socket()
        try:
            # 监听端口
            print('jobrunner listening on ', self.port)
            listen_fd.bind(("0.0.0.0", self.port))
            listen_fd.listen(5)
            while not self.completed:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                try:
                    # 获取请求方发送的指令
                    request = str(sock_fd.recv(128), encoding='utf-8')
                    request = request.split(' ')
                    task_id = int(request[0])
                    task_node = request[1]
                    self.scheduler_time_sum += float(request[2])
                    m = threading.Thread(target=self.launch_task, args=(task_id, task_node, sock_fd))
                    m.daemon = True  # daemon True设置为守护即主死子死.
                    m.start()
                    # res = self.launch_task(task_id, task_node)

                    # response = serialize_data(res)

                    # sock_fd.send(response)

                except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
                    pass
                except Exception as e:  # 如果出错则打印错误信息
                    print(e)
                # finally:
                    # sock_fd.close()  # 释放连接
        except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
            pass
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            listen_fd.close()  # 释放连接

    def run(self):
        self.port = get_free_port()
        self.job.port = self.port
        self.job.populate_tasks()

        # map stage
        threads = []
        t = threading.Thread(target=self.listen_for_scheduler_info)
        threads.append(t)
        # t = threading.Thread(target=self.check_job_status_loop)
        # threads.append(t)
        for t in threads:
            t.start()
        
        # for t in threads:
        #     t.join()
        print('connecting to scheduler')
        scheduler_sock = create_sock('0.0.0.0', scheduler_port)
        request = 'submit_job'
        scheduler_sock.send(bytes(request, encoding='utf-8'))
        while True:
            res = scheduler_sock.recv(BUF_SIZE)
            if str(res, encoding='utf-8') == 'ready':
                break
        # scheduler_sock.close()

        # scheduler_sock = create_sock(main_host, scheduler_port)
        send_data(scheduler_sock, pickle.dumps(self.job)) # submit a list of map tasks

        while True:
            if self.completed:
                print("transmit_time_sum_dcit: {}".format(self.transmit_time_sum_dcit))
                print("data_process_time_sum_dict: {}".format(self.data_process_time_sum_dict))
                print("scheduler_time_sum: {}".format(self.scheduler_time_sum))
                print("run_time: {}".format(time.time() - self.begin_time))
                return self.job.reduce()

    @property    
    def completed(self):
        return self.job.completed
    
    # def check_job_status_loop(self):
        
    #         if self.completed:
    #             return True

    def launch_task(self, task_id, host, sock_fd):
        sock = create_sock(host, data_node_port)

        prefer_nodes = self.job.tasks[task_id].preferred_datanode
        node = host if host in prefer_nodes else prefer_nodes[0]
        request = self.job.get_task_cmd(task_id) + ' {}'.format(node)
        print('Request sent to datanode {}: {}'.format(host, request))
        sock.send(bytes(request, encoding='utf-8'))
        response = receive_data(sock)
        response = deserialize_data(response) # json
        sock.close()
        # print('response:', response)
        if response['status']:
            print('request success')
            self.job.task_complete(task_id)
            self.job.task_result(task_id, response['result'])
            self.data_process_time_sum_dict[host] += float(response['data_process_time'])
            self.transmit_time_sum_dcit[host] += float(response['transmit_time'])
            res = True
        else:
            print('[WARNING] request failed')
            print(response['error'])
            res = False
            "TODO any other possible operations"
        send_data(sock_fd, serialize_data(res))


# # %%
# import pandas as pd
# fat = pd.read_csv('../dfs/name/dataset/newdata.csv')
# job = WordCountJob(1, fat, './dataset/newdata.csv', field_name=2)
# job.port = 12345
# # %%
# job.populate_tasks()
# # %%
# for i in job.tasks:
#     print(i)
#     print(job.tasks[i])
#     print(job.tasks[i].task_cmd)
#     break
# # %%
# job.tasks[0].__dict__
# # %%

# %%
