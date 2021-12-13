# MyDFS
This project implements naive distributed file system (DFS).

## Scheduler Design
### data_node.py
- 加入各datanode之间的通信解决datanode之间的数据传输问题

### Scheduler.py
- base class `Scheduler`
    - class method
        - get_next_task
    - class attribute
        - datanode_load # how many tasks are running for each node
        - datanode_delay # communication time cost for each data node
        - datanode_port
- 实现细节
    - 维持一个pool，包含所有job以及对应task以及提交时间
    - 维护每个datanode正在执行的任务数量以及每个任务开始执行的时间
    - 维护跟每个datanode的通信时延
    - 保存各部分data所在节点位置（fat item）
    - 开一个进程监控datanode正在执行的任务数量，一旦有空余就分配一个任务到该节点
    - 在多个job的情况，考虑加入每个job的worker限制

#### Different Scheduler
- RandomScheduler
    - 总是从pool中随机挑选一个task
- SimpleQueueScheduler
    - 总是perfer data locality，即如果datanode空闲也不一定会分配任务
- QuincyScheduler
    - 一旦有空余就建图计算找到应当被分配的任务
        - 边权值=\alpha * 通信时延 + \beta * 数据传输cost + \gamma * 等待时间
        - 如果每次只调度一个槽位的任务，这个问题好像没那么复杂？


### mapreduce.py
- add scheduler as attribute

### Logic
- mapreduceclient 调用 scheduler，scheduler返回命令执行结果
- scheduler跟每个datanode建立通信（此处需要多线程）
    - scheduler向datanode发布任务，并接收结果（结果包括数据传输cost和实际数据处理结果）
    - scheduler负责计算时间（任务调度时间、实际数据处理时间、总时间）
