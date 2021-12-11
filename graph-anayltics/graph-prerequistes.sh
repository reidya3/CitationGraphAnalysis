$HADOOP_HOME/sbin/start-all.sh
$HADOOP_HOME/bin/hdfs dfs -mkdir input/citation-grap-analysis
$HADOOP_HOME/bin/hdfs dfs -mkdir input/citation-grap-analysis/graphx/
$HADOOP_HOME/bin/hdfs dfs -put  /home/reidya3/cleaned_data/collaborations input/citation-grap-analysis/graphx/
$HADOOP_HOME/bin/hdfs dfs -put  /home/reidya3/cleaned_data/unique_authors input/citation-grap-analysis/graphx/
$HADOOP_HOME/bin/hdfs dfs -put  /home/reidya3/cleaned_data/doras_edges input/citation-grap-analysis/graphx/
$HADOOP_HOME/bin/hdfs dfs -put  /home/reidya3/cleaned_data/doras_vertices input/citation-grap-analysis/graphx/
$HADOOP_HOME/bin/hdfs dfs -ls  input/citation-grap-analysis/graphx/
