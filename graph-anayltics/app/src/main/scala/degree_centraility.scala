import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.Row
import scala.reflect.ClassTag
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.sql.functions.{col, lit, when}


case class Researcher(name: String, position: String)

// Creading RDD for edges i.e. Researcher -> Reseracher where weight is the total number of papers + (0.5) citation count between researchers
object DegreeMain {
	def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Degree centrality").getOrCreate()
    import spark.implicits._  
    spark.sparkContext.setLogLevel("WARN")

    // Schema for researchers.csv file
    val researchersSchemaExpected =  StructType(
        List(
            StructField("index" , LongType , true),
            StructField("Unique_Author" , StringType, true),
            StructField("Job_title" , StringType,  true)
            )
        )
    //defining schema for edge realtionships
    val collabartionsSchemaExpected =  StructType(
        List(
            StructField("Name1" , LongType , true),
            StructField("Name2" , LongType, true),
            StructField("Total_Paper_Count" , LongType,  true),
            StructField("Total_Citations_Achieved" , LongType,  true)
            )
        )

    val collaborations = spark.read.option("header",true).schema(collabartionsSchemaExpected).csv("hdfs://localhost:9000/input/citation-grap-analysis/graphx/collaborations")
    val collaborations_weighted = collaborations.withColumn("weight", (col("Total_Paper_Count")  + (col("Total_Citations_Achieved") * .5))).drop("Total_Paper_Count", "Total_Citations_Achieved")
    val collaborations_weighted_rdd:  RDD[Edge[Double]] = collaborations_weighted.rdd.map { row:Row =>
        Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row.getAs[Double](2))
    }

    val researchers: RDD[(VertexId, Researcher)] = spark.read.option("header",true).schema(researchersSchemaExpected).csv("hdfs://localhost:9000/input/citation-grap-analysis/graphx/unique_authors").rdd.map{x:Row => (x.getAs[VertexId](0),Researcher(x.getAs[String](1),  x.getAs[String](2)))}

    // calculating the degrees each researchers has i.e. number of collaborators. 
    val myGraph: Graph[Researcher, Double] = Graph(researchers, collaborations_weighted_rdd)
    myGraph.vertices.collect.foreach { case (id:Long, Researcher(name, pos)) =>
        var out_degree =  collaborations_weighted_rdd.filter{case Edge(srcid:Long, dstid:Long, weight ) => srcid == id}.count
        var in_degree =  collaborations_weighted_rdd.filter{case Edge(srcid:Long, dstid:Long, weight ) => dstid == id}.count
        var total_degree = out_degree + in_degree
        println(name, total_degree)
    }

    // caculating degress within the dcu community only 
    //initializing the graph (dcu researchers only, directed)
    val dcu_researhers_only_vertex_list = researchers.filter{ case(id, Researcher(name, pos)) => pos != "Unkown"}.map(x => x._1).collect
    val dcu_constricted_vertex = myGraph.vertices.filter{ case (id:Long, Researcher(name, pos)) => dcu_researhers_only_vertex_list.contains(id)}
    val dcu_constricted_edges = myGraph.edges.filter{ case Edge(srcid:Long, dstid:Long, weight ) => dcu_researhers_only_vertex_list.contains(srcid) && dcu_researhers_only_vertex_list.contains(dstid)}
    val dcu_only_graph = Graph(dcu_constricted_vertex, dcu_constricted_edges)

    // calculating the degrees each researchers has i.e. number of collaborators. 
    println("Compute degree centraility for DCU researcher only graph")
    dcu_only_graph.vertices.collect.foreach { case (id:Long, Researcher(name, pos)) =>
        var out_degree =  dcu_only_graph.edges.filter{case Edge(srcid:Long, dstid:Long, weight ) => srcid == id}.count
        var in_degree =  dcu_only_graph.edges.filter{case Edge(srcid:Long, dstid:Long, weight ) => dstid == id}.count
        var total_degree = out_degree + in_degree
        println(name, total_degree)
    }
    spark.stop()
	}
}