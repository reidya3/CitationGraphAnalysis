import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import spark.implicits._ 
import org.apache.spark.sql.Row
import scala.reflect.ClassTag
import org.apache.spark.util.collection.OpenHashSet

// Schema for researchers.csv file
val researchersSchemaExpected =  StructType(
    List(
        StructField("index" , LongType , true),
        StructField("Unique_Author" , StringType, true),
        StructField("Job_title" , StringType,  true)
        )
    )

// Schema for collaboration edges
val collabartionsSchemaExpected =  StructType(
    List(
        StructField("Name1" , LongType , true),
        StructField("Name2" , LongType, true),
        StructField("Total_Paper_Count" , LongType,  true),
        StructField("Total_Citations_Achieved" , LongType,  true)
        )
    )

/
case class Researcher(name: String, position: String)

// defining Edge RDD and duplicating Edges so its bi-directional or undirected. 
val collaborations = spark.read.option("header",true).schema(collabartionsSchemaExpected).csv("CitationGraphAnalysis/graph-anayltics/cleaned_data/collaborations.csv")
val collaborations_weighted = collaborations.withColumn("weight", (col("Total_Paper_Count")  + (col("Total_Citations_Achieved") * .5))).drop("Total_Paper_Count", "Total_Citations_Achieved")
val collaborations_weighted_rdd:  RDD[Edge[Double]] = collaborations_weighted.rdd.map { row:Row =>
      Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row.getAs[Double](2))
}
val collaborations_weighted_rdd_other_direction:  RDD[Edge[Double]] = collaborations_weighted.rdd.map { row:Row =>
      Edge(row.getAs[VertexId](1), row.getAs[VertexId](0), row.getAs[Double](2))}
val collaborations_bi_directed = collaborations_weighted_rdd.union(collaborations_weighted_rdd)

// Defining reseracher vertices RDD
val researchers: RDD[(VertexId, Researcher)] = spark.read.option("header",true).schema(researchersSchemaExpected).csv("CitationGraphAnalysis/graph-anayltics/cleaned_data/unique_authors.csv").rdd.map{x:Row => (x.getAs[VertexId](0),Researcher(x.getAs[String](1),  x.getAs[String](2)))}

// Undirected graph of DCU researchers plus external collaborators
val undirected_graph: Graph[Researcher, Double] = Graph(researchers, collaborations_bi_directed)
val ranks = undirected_graph.pageRank(0.0001).vertices

println("Researcher's pagerank scores (including outside collaborators")
// Join the ranks with the usernames
val researchers_rank = researchers.join(ranks)
researchers_rank.collect().foreach(println)


// compute dcu researcher only graph
println("DCU researcher ony")
val dcu_researhcers_only_vertex_list = researchers.filter{ case(id, Researcher(name, pos)) => pos != "Unkown"}.map(x => x._1).collect
val dcu_constricted_vertex = undirected_graph.vertices.filter{ case (id:Long, Researcher(name, pos)) => dcu_researhcers_only_vertex.contains(id)}
val dcu_constricted_edges = undirected_graph.edges.filter{ case Edge(srcid:Long, dstid:Long, weight ) => dcu_researhcers_only_vertex.contains(srcid) && dcu_researhcers_only_vertex.contains(dstid)}
val dcu_only_graph = Graph( dcu_constricted_vertex, dcu_constricted_edges)
//Compute page rank of dcu researchers only
val dcu_only_rank = dcu_only_graph.pageRank(0.0001).vertices
val researchers_rank = researchers.join(dcu_only_rank)
// Compute pagerank of researchers only
researchers_rank.collect().foreach(println)

