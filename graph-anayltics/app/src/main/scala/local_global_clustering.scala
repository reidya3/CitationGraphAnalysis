import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.Row
import scala.reflect.ClassTag
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};
import org.apache.spark.sql.Row
import scala.reflect.ClassTag
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row
import scala.reflect.ClassTag
import org.apache.spark.sql.functions.{col, lit, when}

object LocalClusteringCoefficient {
  /**
   * Compute the local clustering coefficient for each vertex and
   * return a graph with vertex value representing the local clustering coefficient of that vertex
   *
   * @param graph the graph for which to compute the connected components
   *
   * @return a graph with vertex attributes containing
   *         the local clustering coefficient of that vertex
   *
   */
  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Double, ED] = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // Construct map representations of the neighborhoods
    // key in the map: vertex ID of a neighbor
    // value in the map: number of edges between the vertex and the corresonding nighbor
    val nbrSets: VertexRDD[Map[VertexId, Int]] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        var nbMap = Map.empty[VertexId, Int]
        var i = 0
        while (i < nbrs.size) {
          // prevent self cycle
          val nbId = nbrs(i)
          if(nbId != vid) {
            val count = nbMap.getOrElse(nbId, 0)
            nbMap += (nbId -> (count + 1))
          }
          i += 1
        }
        nbMap
      }

    // join the sets with the graph
    val setGraph: Graph[Map[VertexId, Int], ED] = g.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }

    // map function: for each edge (srcId = v_i, dstId = v_j),
    // compute |{e_jk: e_ik, e_jk \in E, v_k \in V}| for vertex v_i
    // and |{e_ik: e_ik, e_jk \in E, v_k \in V}| for vertex v_j
    def edgeFunc(ctx: EdgeContext[Map[VertexId, Int], ED, Double]) {
      assert(ctx.srcAttr != null)
      assert(ctx.dstAttr != null)
      // make sure srcId != dstId
      if (ctx.srcId == ctx.dstId) {
        return
      }
      // handle duplated edges
      if ((ctx.srcAttr(ctx.dstId) == 2 && ctx.srcId > ctx.dstId) || (ctx.srcId == ctx.dstId)) {
        return
      }

      val (smallId, largeId, smallMap, largeMap) = if (ctx.srcAttr.size < ctx.dstAttr.size) {
        (ctx.srcId, ctx.dstId, ctx.srcAttr, ctx.dstAttr)
      } else {
        (ctx.dstId, ctx.srcId, ctx.dstAttr, ctx.srcAttr)
      }
      val iter = smallMap.iterator
      var smallCount: Int = 0
      var largeCount: Int = 0
      while (iter.hasNext) {
        val valPair = iter.next()
        val vid = valPair._1
        val smallVal = valPair._2
        val largeVal = largeMap.getOrElse(vid, 0)
        if (vid != ctx.srcId && vid != ctx.dstId && largeVal > 0) {
          smallCount += largeVal
          largeCount += smallVal
        }
      }
      if (ctx.srcId == smallId) {
        ctx.sendToSrc(smallCount)
        ctx.sendToDst(largeCount)
      } else {
        ctx.sendToDst(smallCount)
        ctx.sendToSrc(largeCount)
      }
    }

    // compute |{e_jk: v_j, v_k \in N_i, e_jk \in E}| for each vertex i
    val counters: VertexRDD[Double] = setGraph.aggregateMessages(edgeFunc, _ + _)

    // count number of neighbors for each vertex
    var nbNumMap = Map[VertexId, Int]()
    nbrSets.collect().foreach { case (vid, nbVal) =>
      nbNumMap += (vid -> nbVal.size)
    }

    // Merge counters with the graph
    g.outerJoinVertices(counters) {
      (vid, _, optCounter: Option[Double]) =>
        val dblCount: Double = optCounter.getOrElse(0)
        val nbNum = nbNumMap(vid)
        assert((dblCount.toInt & 1) == 0)
        if (nbNum > 1) {
          dblCount / (2 * nbNum * (nbNum - 1))
        }
        else {
          0
        }
    }
  }
}

object LocalGlobalClusteringMain {
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

    val collabartionsSchemaExpected =  StructType(
        List(
            StructField("Name1" , LongType , true),
            StructField("Name2" , LongType, true),
            StructField("Total_Paper_Count" , LongType,  true),
            StructField("Total_Citations_Achieved" , LongType,  true)
            )
        )


    case class Researcher(name: String, position: String)

    // collaborations csv details edges in the graph
    val collaborations = spark.read.option("header",true).schema(collabartionsSchemaExpected).csv("/usr/local/spark/CitationGraphAnalysis/graph-anayltics/cleaned_data/collaborations.csv")
    val collaborations_weighted = collaborations.withColumn("weight", (col("Total_Paper_Count")  + (col("Total_Citations_Achieved") * .5))).drop("Total_Paper_Count", "Total_Citations_Achieved")
    val collaborations_weighted_rdd:  RDD[Edge[Double]] = collaborations_weighted.rdd.map { row:Row =>
        Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row.getAs[Double](2))
    }

    // unique authors csv details vertices in the graph
    val researchers: RDD[(VertexId, Researcher)] = spark.read.option("header",true).schema(researchersSchemaExpected).csv("/usr/local/spark/CitationGraphAnalysis/graph-anayltics/cleaned_data/unique_authors.csv").rdd.map{x:Row => (x.getAs[VertexId](0),Researcher(x.getAs[String](1),  x.getAs[String](2)))}


    //initializing the graph (dcu + external researchers, directed)
    val myGraph: Graph[Researcher, Double] = Graph(researchers, collaborations_weighted_rdd)

    //compute local clustering coefficient and triangle count for each researcher
    val lcc = LocalClusteringCoefficient.run(myGraph)
    val verts = lcc.vertices
    val triCounts = myGraph.triangleCount().vertices
    val tricount_local_coeff_by_user = verts.join(triCounts)
    println("Local clustering coefficient and Triangle count for each researcher")

    tricount_local_coeff_by_user.collect.foreach { case (vid, (cluster_coef,triangle_count)) =>
    var researcher_vertex = myGraph.vertices.filter{ case (id:Long, Researcher(name, pos)) => id == vid}
    researcher_vertex.collect.foreach { case (id:Long, Researcher(name, pos)) =>
        println(name, (cluster_coef,triangle_count))
            }
    }

    // compute global clustering coefficient 
    var global_clustering_coefficent = 0.0
    verts.collect.foreach { case (vid, count) =>
    global_clustering_coefficent +=  count
    }
    println("Global clustering coefficient", global_clustering_coefficent/verts.count )

    //initializing the graph (dcu researchers only, directed)
    val dcu_researhcers_only_vertex_list = researchers.filter{ case(id, Researcher(name, pos)) => pos != "Unkown"}.map(x => x._1).collect
    val dcu_constricted_vertex = myGraph.vertices.filter{ case (id:Long, Researcher(name, pos)) => dcu_researhcers_only_vertex_list.contains(id)}
    val dcu_constricted_edges = myGraph.edges.filter{ case Edge(srcid:Long, dstid:Long, weight ) => dcu_researhcers_only_vertex_list.contains(srcid) && dcu_researhcers_only_vertex_list.contains(dstid)}
    val dcu_only_graph = Graph( dcu_constricted_vertex, dcu_constricted_edges)
    //compute local clustering coefficient and triangle count for each researcher
    val lcc_dcu_only = LocalClusteringCoefficient.run(dcu_only_graph)
    val verts_dcu_only = lcc_dcu_only.vertices
    val triCounts_dcu_only = dcu_only_graph.triangleCount().vertices
    val tricount_local_coeff_by_user_dcu_only = verts_dcu_only.join(triCounts)
    println("Local clustering coefficient and Triangle count for each researcher")

    tricount_local_coeff_by_user_dcu_only.collect.foreach { case (vid, (cluster_coef,triangle_count)) =>
    var researcher_vertex = dcu_only_graph.vertices.filter{ case (id:Long, Researcher(name, pos)) => id == vid}
    researcher_vertex.collect.foreach { case (id:Long, Researcher(name, pos)) =>
        println(name, (cluster_coef,triangle_count))
            }
    }

    // compute global clustering coefficient 
    var global_clustering_coefficent_dcu_only = 0.0
    verts.collect.foreach { case (vid, count) =>
    global_clustering_coefficent +=  count
    }
    println("Global clustering coefficient", global_clustering_coefficent_dcu_only/verts_dcu_only.count)
   }
}

