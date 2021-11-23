# Details:
| Folder/file | Description |
|---:|:---|
| app| A scala sbt app that contains the graph algorithms. `Sbt assembly` is used to package the files into a jar file. |
|Cleaned data  | CSV files needed to construct *Graph_1*, *Graph_2* and *Graph_3* |
|run-graph-algo.sh  | Contains the necessary bash commands required to start the spark cluster and run the graph algorithms via the spark-submit command. |
| Spark-shell-files  | The initial scala files used to construct the graph algorithms which were inputted into spark-shell. These files are *now deprecated*. |
| cypher-queries.cypher  | As we could not find a working implementation of Louvian Modulaity or betweens centrality on GraphX, we decide to use Neo4j to implement them. This file contains the necessary cypher queries. |
