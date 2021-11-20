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

val collabartionsSchemaExpected =  StructType(
    List(
        StructField("Name1" , LongType , true),
        StructField("Name2" , LongType, true),
        StructField("Total_Paper_Count" , LongType,  true),
        StructField("Total_Citations_Achieved" , LongType,  true)
        )
    )


case class Researcher(name: String, position: String)

val collaborations = spark.read.option("header",true).schema(collabartionsSchemaExpected).csv("CitationGraphAnalysis/graph-anayltics/cleaned_data/collaborations.csv")
val collaborations_weighted = collaborations.withColumn("weight", (col("Total_Paper_Count")  + (col("Total_Citations_Achieved") * .5))).drop("Total_Paper_Count", "Total_Citations_Achieved")
val collaborations_weighted_rdd:  RDD[Edge[Double]] = collaborations_weighted.rdd.map { row:Row =>
      Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row.getAs[Double](2))
}

// bidirected 
//val collaborations_weighted_rdd_other_direction:  RDD[Edge[Double]] = collaborations_weighted.rdd.map { row:Row =>
//      Edge(row.getAs[VertexId](1), row.getAs[VertexId](0), row.getAs[Double](2))
//}
//val collaborations_bi_directed = collaborations_weighted_rdd.union(collaborations_weighted_rdd)

//val researchers: RDD[(IntegerType, StringType, StringType)] = spark.read.option("header",true).schema(researchersSchemaExpected).csv("CitationGraphAnalysis/graph-anayltics/cleaned_data/unique_authors.csv").rdd.map{x:Row => (x.getAs[IntegerType](0),x.getAs[StringType](1),  x.getAs[StringType](2))}

val researchers: RDD[(VertexId, Researcher)] = spark.read.option("header",true).schema(researchersSchemaExpected).csv("CitationGraphAnalysis/graph-anayltics/cleaned_data/unique_authors.csv").rdd.map{x:Row => (x.getAs[VertexId](0),Researcher(x.getAs[String](1),  x.getAs[String](2)))}
researchers.collect.foreach { case (id:Long, Researcher(name, pos)) =>
}


val myGraph: Graph[Researcher, Double] = Graph(researchers, collaborations_weighted_rdd)
myGraph.vertices.collect.foreach { case (id:Long, Researcher(name, pos)) =>
    var out_degree =  collaborations_weighted_rdd.filter{case Edge(srcid:Long, dstid:Long, weight ) => srcid == id}.count
    var in_degree =  collaborations_weighted_rdd.filter{case Edge(srcid:Long, dstid:Long, weight ) => dstid == id}.count
    var total_degree = out_degree + in_degree
    println(name, total_degree)
}


