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
val keywordSchemaExpected =  StructType(
    List(
        StructField("id" , LongType , true),
        StructField("keyword" , StringType, true),
        )
    )

val keyEdgesSchemaExpected =  StructType(
    List(
        StructField("keyword_1" , LongType , true),
        StructField("keyword_2" , LongType, true),
        StructField("count" , DoubleType,  true),
        )
    )


case class Keyword(value: String)

// collaborations csv details edges in the graph
val key_edges = spark.read.option("header",true).schema(keyEdgesSchemaExpected).csv("CitationGraphAnalysis/graph-anayltics/cleaned_data/doras_edges.csv")
val key_edges_weighted_rdd:  RDD[Edge[Double]] = key_edges.rdd.map { row:Row =>
      Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row.getAs[Double](2))
}

// unique authors csv details vertices in the graph
val keyword: RDD[(VertexId, Keyword)] = spark.read.option("header",true).schema(keywordSchemaExpected).csv("CitationGraphAnalysis/graph-anayltics/cleaned_data/doras_vertices.csv").rdd.map{x:Row => (x.getAs[VertexId](0),Keyword(x.getAs[String](1)))}


//initializing the graph (dcu + external researchers, directed)
val doras_keyword_graph: Graph[Keyword, Double] = Graph(keyword, key_edges_weighted_rdd)

//compute local clustering coefficient and triangle count for each researcher
val doras_label_progatted = lib.LabelPropagation.run(doras_keyword_graph, 10)

doras_label_progatted.vertices.collect.foreach{println}