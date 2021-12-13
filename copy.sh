for host in 'thumm02' 'thumm03' 'thumm04' 'thumm05';
do
scp client.py 2021214296@$host:MyDFS/client.py
scp common.py 2021214296@$host:MyDFS/common.py
scp data_node.py 2021214296@$host:MyDFS/data_node.py
scp name_node.py 2021214296@$host:MyDFS/name_node.py
done