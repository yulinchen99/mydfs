from asset import create_sock, deserialize_data, get_free_port, send_data, serialize_data
import socket
from ..common import *
import time
import threading
import pickle

class Task:
    def __init__(self, priority, preferred_datanode, cmd, port, task_id):
        self.priority = priority
        self.preferred_datanode = preferred_datanode
        self.cmd = cmd
        self.completed = False
        self.result = None
        self.timestamp = time.time()
        self.port = port
        self.task_id = task_id
        self._task_cmd = None

    @property
    def task_cmd(self):
        if not self._task_cmd:
            "TODO customize cmd for different task"
            self._task_cmd = 'cmd'
        return self._task_cmd
    

class Job:
    r"""
    This is only applicable to Jobs that involve one map stage + one reduce stage
    """
    def __init__(self, jobid, command, fat):
        self.jobid = jobid
        self.command = command
        self.fat = fat
        self.completed = False
        self.completed_cnt = 0
        self.tasks = self.populate_tasks()
    
    @property
    def completed(self):
        return self.completed_cnt == len(self.tasks)

    def populate_tasks(self):
        "TODO populate map task list for different jobs"
        pass

    def task_complete(self, task_id):
        if not self.tasks[task_id].completed:
            self.tasks[task_id].completed = True
            self.completed_cnt += 1
    
    def task_result(self, task_id, result):
        self.tasks[task_id].result = result
    
    def get_task_cmd(self, task_id):
        return self.tasks[task_id].get_task_cmd()

    def reduce(self):
        "TODO customize reduce method for different jobs"
        pass

    
class JobRunner:
    def __init__(self, job: Job):
        self.port = get_free_port()
        self.job = job
        self.completed_cnt = 0
    
    def listen_for_scheduler_info(self):
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", jobrunner_port))
            listen_fd.listen(5)
            print("Scheduler started")
            while not self.completed:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                try:
                    # 获取请求方发送的指令
                    request = str(sock_fd.recv(128), encoding='utf-8')
                    request = request.split(' ')
                    task_id = int(request[0])
                    task_node = request[1]
                    res = self.launch_task(task_id, task_node)

                    response = serialize_data(res)

                    sock_fd.send(response)

                except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
                    pass
                except Exception as e:  # 如果出错则打印错误信息
                    print(e)
                finally:
                    listen_fd.close()  # 释放连接
        except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
            pass
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            listen_fd.close()  # 释放连接

    def run(self):
        scheduler_sock = create_sock(main_host, scheduler_port)
        request = 'submit_job'
        scheduler_sock.send(bytes(request, encoding='utf-8'))
        while True:
            res = scheduler_sock.recv(BUF_SIZE)
            if str(res, encoding='utf-8') == 'ready':
                break
        scheduler_sock.close()

        scheduler_sock = create_sock(main_host, scheduler_port)
        send_data(scheduler_sock, pickle.dumps(self.job)) # submit a list of map tasks

        # map stage
        threads = []
        t = threading.Thread(target=self.listen_for_scheduler_info)
        threads.append(t)
        t = threading.Thread(target=self.check_job_status_loop)
        threads.append(t)
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
        
        # reduce stage
        return self.job.reduce()
        

    @property    
    def completed(self):
        return self.job.completed
    
    def check_job_status_loop(self):
        while True:
            time.sleep(1)
            if self.completed:
                return True

    def launch_task(self, task_id, host):
        try:
            sock = create_sock(host, data_node_port)
            # add host
            request = self.job.get_task_cmd(task_id) + f' {host}'
            sock.send(bytes(request, encoding='utf-8'))
            response = sock.recv(BUF_SIZE)
            sock.close()
            response = deserialize_data(response) # json
            if response['status']:
                self.job.task_complete(task_id)
                self.job.task_result(task_id, response['result'])
            "TODO any other possible operations"
            return True
        except Exception as e:
            print(e)
            return False


