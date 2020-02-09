export HDFS_NAMENODE_USER=lavish
export HDFS_DATANODE_USER=lavish
export HDFS_SECONDARYNAMENODE_USER=lavish
export YARN_RESOURCEMANAGER_USER=lavish
export YARN_NODEMANAGER_USER=lavish
export PDSH_RCMD_TYPE=ssh

cd $SPARK_HOME
./sbin/start-master.sh
./sbin/start-slave.sh spark:7077

ps -aef | grep spark

cat /opt/spark/logs/spark-lavish-org.apache.spark.deploy.worker.Worker-1-hadoop.out


hdfs dfs -ls /user

bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar grep input output 'dfs[a-z.]+'