## Build jar file
cd app/ && sbt assembly

# go back of app
cd ..

## run graph algorithims as if the PWD was at this location i.e. CitationGraphAnalysis/grah-anayltics/

# Start master node. 
$SPARK_HOME/sbin/start-master.sh

# Run worker 
$SPARK_HOME/sbin/start-worker.sh  spark://LAPTOP-I3QDVRGC.localdomain:7077

# Run degree centraility 
$SPARK_HOME/bin/spark-submit --class DegreeMain --master local[4] app/target/scala-2.12/Graph\ Algos-assembly-1.4.9.jar

# Run label propagation 
$SPARK_HOME/bin/spark-submit --class LabelPropagationMain --master local[4] app/target/scala-2.12/Graph\ Algos-assembly-1.4.9.jar

# Run  global, local and traingular counts for researchers
$SPARK_HOME/bin/spark-submit --class LocalGlobalClusteringMain --master local[4] app/target/scala-2.12/Graph\ Algos-assembly-1.4.9.jar

# Run page rank for researchers
$SPARK_HOME/bin/spark-submit --class PageRankMain --master local[4] app/target/scala-2.12/Graph\ Algos-assembly-1.4.9.jar
