# MyDFS
This project implements naive distributed file system (DFS).

## The files concerned
- util
    - asset.py # util function
    - job.py # job, jobrunner and task related
- common.py # hyper-param
- data_node.py # 标注了wordcount的部分代码
- scheduler.py
- wordcount.py # client entrance script
- name_node.py

*all other files should not be modified*


## TODO
### basics
- [ ] QuincyScheduler
- [ ] communication between datanodes
    - [x] basics
    - [ ] multi-processing problem (one datanode doing two jobs at the same time)
        - random scheduler 似乎会卡住（死锁？）
- [x] task specific api in data node and client
- [x] connect client to jobrunner (one client corresponds to one jobrunner, so directly make jobrunner an attribute of client would work?)
- [ ] task specific operation in `util/job.py`
    - [x] word count operation
- [ ] metrics calculation (任务调度时间、实际数据处理时间、数据传输等)
- [x] multi-process problem (one datanode doing two jobs at the same time)
    - [x] datanode
    - [x] jobrunner

### advanced
- [ ] task/job priority
- [ ] ...

## Done
- scheduler base class
- job and task class in `util/job.py`
- Random Scheduler
- Queue Scehduler based on submitting time and data locality (tested)

## Scheduler Design
### data_node.py
- 加入各datanode之间的通信解决datanode之间的数据传输问题

### Scheduler.py
- 实现细节
    - 维持一个pool，包含所有job以及对应task以及提交时间
    - 维护每个datanode正在执行的任务数量以及每个任务开始执行的时间
    - 维护每个job的各种cost
    - 维护跟每个datanode的通信时延
    - 开一个进程监控datanode正在执行的任务数量，一旦有空余就分配一个任务到该节点
    - *在多个job的情况，考虑加入每个job的worker限制

#### Different Scheduler
- RandomScheduler
    - 总是从pool中随机挑选一个task
- DataLocalityScheduler
    - 总是perfer data locality，即如果datanode空闲也不一定会分配任务
- QuincyScheduler
    - 一旦有空余就建图计算找到应当被分配的任务
        - cost计算示例：边权值=\alpha * 通信时延 + \beta * 数据传输cost + \gamma * 等待时间

### Logic
- mapreduceclient 调用 jobrunner，每个jobrunner执行一个job并分配一个空闲端口
- jobrunner跟scheduler进行通信，提交需要执行的任务。scheduler会不停地schdule task，并向对应的jobrunnner发送执行task的命令
- jobrunner收到scheduler指令之后，跟对应datanode建立通信发送任务执行指令（此处需要多线程）
    - jobrunner向datanode发布任务，并接收结果（结果包括数据传输cost和实际数据处理结果）
- scheduler负责计算时间（任务调度时间、实际数据处理时间）

### Usage
```sh
cd dataset
sh download.sh
cd ..
sh storedata.sh
python scheduler.py # start scheduler, default datalocality scheduler
python wordcount.py -wc ./dataset/newdata.csv 2
# *python wordcount.py -wc <file_path> <column_id>*
```