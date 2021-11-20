$HADOOP_HOME/sbin/start-all.sh
$HADOOP_HOME/bin/hdfs dfs -mkdir input/citation-grap-analysis
$HADOOP_HOME/bin/hdfs dfs -mkdir input/citation-grap-analysis/graphx/
$HADOOP_HOME/bin/hdfs dfs -put  /home/reidya3/ml-latest-small/ratings.csv  input/citation-grap-analysis/graphx/
$HADOOP_HOME/bin/hdfs dfs -put  /home/reidya3/ml-latest-small/tags.csv  input/citation-grap-analysis/graphx/
$HADOOP_HOME/bin/hdfs dfs -ls  input/citation-grap-analysis/graphx/
