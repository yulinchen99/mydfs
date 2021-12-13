netstat -anp|grep 14297|awk '{print $7}'|awk -F '/' '{print $1}'|xargs kill -s 9
netstat -anp|grep 14296|awk '{print $7}'|awk -F '/' '{print $1}'|xargs kill -s 9
netstat -anp|grep 14295|awk '{print $7}'|awk -F '/' '{print $1}'|xargs kill -s 9
