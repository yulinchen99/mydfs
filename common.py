dfs_replication = 2
dfs_blk_size = 40960000  # * 1024

# NameNode和DataNode数据存放位置
name_node_dir = "./dfs/name"
data_node_dir = "./dfs/data"
checksum_node_dir = "./dfs/checksum"

data_node_port = 15297  # DataNode程序监听端口
name_node_port = 15296  # NameNode监听端口
heartbeat_port = 15295
heartbeat_interval = 30 # second

# 集群中的主机列表
host_list =   ['thumm01', 'thumm02', 'thumm03', 'thumm04', 'thumm05']#, 'thumm03', 'thumm04', 'thumm05'] # ['localhost']
name_node_host = "thumm01"

BUF_SIZE = 40960

main_host = "thumm01"

# scheduler
scheduler_port = 15299
max_load = 1

# job runner
jobrunner_port = 15300