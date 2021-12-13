from common import *

class Scheduler:
    def __init__(self):
        self.datanode_load = {'thumm01':0,}
        self.datanode_delay = {}
        self.datanode_port = data_node_port
        pass
    
    def _get_datanode_delay(self):
        # reset datanode_delay
        pass