sudo apt update
sudo apt upgrade

sudo apt install openjdk-11-jre-headless

sudo apt-get install ssh
# pdsh: Terminal Command: 
sudo apt-get install pdsh

ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys

export SPARK_HOME=/home/lavish/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export HADOOP_HOME=/home/lavish/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip="192.168.8.143"'
. ~/.bashrc

tar -xvf hadoop.tar.gz
tar -xvf spark.tgz

$HADOOP_HOME/bin/hdfs namenode -format

$HADOOP_HOME/sbin/start-dfs.sh

$SPARK_HOME/sbin/start-master.sh 

$SPARK_HOME/sbin/start-slave.sh spark://ubuntu:7077

pyspark

load data into HDFS
access it from PySpark and display
install cern db keras lib

hadoop fs -copyFromLocal /home/lavish/startUpFunding/data/* /user/lavish/data 

install pip 
pip3 install notebook
pip3 install jupyter

jupyter notebook --ip="192.168.8.136"

git config --global user.email "lavishthomas@gmail.com"
git config --global user.name "Lavish Thomas"

git clone git@github.com:lavishthomas/startupFunding.git


git add *
git add -A

git add $(git rev-parse --show-toplevel)

git commit -m "data check in"
git push origin master


git pull origin master
git add -A
git commit -m "Updated ReadMe"
git push origin master

sudo python3 -m pip uninstall pip && sudo apt install python3-pip --reinstall
pip3 install --user --upgrade tensorflow  # install in $HOME
sudo pip3 install keras


KAFKA_HOME=/home/lavish/pr/kafka
$HADOOP_HOME/sbin/start-dfs.sh &
pyspark &

$KAFKA_HOME/bin/zookeeper-server-start.sh /home/lavish/pr/kafka/config/zookeeper.properties
$KAFKA_HOME/bin/zookeeper-server-stop.sh

$KAFKA_HOME/bin/kafka-server-start.sh /home/lavish/pr/kafka/config/server.properties
$KAFKA_HOME/bin/kafka-server-stop.sh

$KAFKA_HOME/bin/kafka-topics.sh --list


$KAFKA_HOME/bin/kafka-topics.sh --create --topic pr --zookeeper localhost:2181 --partitions 3 --replication-factor 2


$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic pr

$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-kafka-topic --from-beginning


spark-submit --class org.pramerica.sparkHello --master local[8] Intern.jar


