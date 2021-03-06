import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.Row
import scala.reflect.ClassTag
import org.apache.spark.util.collection.OpenHashSet
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row
import scala.reflect.ClassTag
import org.apache.spark.sql.functions.{col, lit, when}

    
case class Keyword(keyword_value: String)

object LabelPropagationMain {
	def main(args: Array[String]):Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Degree centrality").getOrCreate()
    import spark.implicits._  
    spark.sparkContext.setLogLevel("WARN")    
// Schema for keyword vertices file
    val keywordSchemaExpected =  StructType(
        List(
            StructField("id" , LongType , true),
            StructField("keyword" , StringType, true),
            )
        )
    // Schema for keyword edges file
    val keyEdgesSchemaExpected =  StructType(
        List(
            StructField("keyword_1" , LongType , true),
            StructField("keyword_2" , LongType, true),
            StructField("count" , DoubleType, true),
            )
        )


    // creating RDD containing Keyword->Keyword relationships where count is the number of common papers between keywords
    val key_edges = spark.read.option("header",true).schema(keyEdgesSchemaExpected).csv("hdfs://localhost:9000/input/citation-grap-analysis/graphx/doras_edges")
    val key_edges_weighted_rdd:  RDD[Edge[Double]] = key_edges.rdd.map { row:Row =>
        Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row.getAs[Double](2))
    }

    // Creating RDD for keyword vertices
    val keyword: RDD[(VertexId, Keyword)] = spark.read.option("header",true).schema(keywordSchemaExpected).csv("hdfs://localhost:9000/input/citation-grap-analysis/graphx/doras_vertices").rdd.map{x:Row => (x.getAs[VertexId](0),Keyword(x.getAs[String](1)))}


    //initializing the graph (keyword, directed). Label propagation doesn't care about directions, so this is ok.
    val doras_keyword_graph: Graph[Keyword, Double] = Graph(keyword, key_edges_weighted_rdd)

    //find communtites via label propagation algo.
    val doras_label_propagation = lib.LabelPropagation.run(doras_keyword_graph, 10)


    //Print out keyword name and its corresponding community 
    doras_label_propagation.vertices.collect.foreach{case(vid, community) =>
    var keyword_vertex = doras_keyword_graph.vertices.filter{ case (id:Long, Keyword(vakeyword_valuelue)) => id == vid}
    keyword_vertex.collect.foreach{ case (id:Long, Keyword(keyword_value)) =>
        println(keyword_value, community)
            }
    }
    }
}

 