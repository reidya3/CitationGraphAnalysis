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
import scala.collection.mutable.Stack
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.PairRDDFunctions

object KBetweenness {
  def run[VD: ClassTag, ED: ClassTag](
            graph: Graph[VD, ED], k: Int): Graph[Double, Double] =
  {
      val kGraphletsGraph =
        createKGraphlets(graph, k)
        
      val vertexKBcGraph =
        kGraphletsGraph
        .mapVertices((id, attr) => (id, computeVertexBetweenessCentrality(id, attr._2, attr._3)))
    
      val kBCGraph: Graph[Double, Double] = 
        aggregateGraphletsBetweennessScores(vertexKBcGraph)
      
      kBCGraph
  }
  
  def createKGraphlets[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], k: Int): Graph[(Double, List[VertexId], List[(VertexId, VertexId)]), Double] = 
  {
    val graphContainingGraphlets : Graph[(Double, List[VertexId], List[(VertexId, VertexId)]), Double] =
      graph
      // Init edges to hold - Edge Betweenness to 0.0
      .mapTriplets[Double]({x: EdgeTriplet[VD, ED] => (0.0) })
      // Init vertices to hold - Vertex betweenness (0.0), and K distance Edge list (empty)
      .mapVertices( (id, attr) => (0.0, List[VertexId](id), List[(VertexId, VertexId)]()))
      //.reverse // Because GraphX is directed and we want graphlets containing all vertices a vertex might effect 
      .cache()
      
      def vertexProgram(id: VertexId, attr: (Double, List[VertexId], List[(VertexId, VertexId)]), msgSum: (List[VertexId], List[(VertexId, VertexId)])): (Double, List[VertexId], List[(VertexId, VertexId)]) =
        (attr._1, attr._2.union(msgSum._1).distinct, attr._3.union(msgSum._2).distinct)
      def sendMessage(edge: EdgeTriplet[(Double, List[VertexId], List[(VertexId, VertexId)]), Double]) : Iterator[(VertexId, (List[VertexId], List[(VertexId, VertexId)]))]=
        Iterator((edge.dstId, (edge.srcAttr._2.:+(edge.srcId) , edge.srcAttr._3.+:(edge.srcId, edge.dstId))),
                 (edge.srcId, (edge.dstAttr._2.:+(edge.dstId) , edge.dstAttr._3.+:(edge.srcId, edge.dstId)))
                )
      def messageCombiner(a: (List[VertexId], List[(VertexId, VertexId)]), b: (List[VertexId], List[(VertexId, VertexId)])): (List[VertexId], List[(VertexId, VertexId)]) = 
        (a._1.union(b._1) , a._2.union(b._2) )
      // The initial message received by all vertices in PageRank
      val initialMessage = (List[VertexId](), List[(VertexId, VertexId)]())

      // Execute pregel for k iterations, get all vertices/edges in distance k for every node
      Pregel(graphContainingGraphlets, initialMessage, k, activeDirection = EdgeDirection.Both)(
        vertexProgram, sendMessage, messageCombiner)
      //.reverse // return to originial directon
  }
  
  def computeVertexBetweenessCentrality(id: VertexId, vlist: List[VertexId], elist: List[(VertexId, VertexId)]):  List[(VertexId, Double)] = 
  {
    // Init data structures
    val s = Stack[VertexId]()
    val q = Queue[VertexId]()
    
    val dist = new HashMap[VertexId, Double]()
    val sigma = new HashMap[VertexId, Double]()
    val delta = new HashMap[VertexId, Double]()
    val predecessors = new HashMap[VertexId, ListBuffer[VertexId]]()
    val neighbourMap : HashMap[VertexId, List[VertexId]] = getNeighbourMap(vlist,elist)
    val medBC = new ListBuffer[(VertexId, Double)]()
      
    for (vertex <- vlist)
    {
      dist.put(vertex, Double.MaxValue)
      sigma.put(vertex, 0.0)
      delta.put(vertex, 0.0)
      predecessors.put(vertex, ListBuffer[VertexId]())
    }
    
    // Init values before first iteration
    dist(id) = 0.0
    sigma(id) = 1.0
    q.enqueue(id)
    
    // Go over all queued vertices
    while (!(q.isEmpty))
    {
      val v = q.dequeue()
      s.push(v)
      // Go over v's neighbours 
      for (w <- neighbourMap(v))
      {
        if (dist(w) == Double.MaxValue)
        {
          dist(w) = dist(v) + 1.0
          q.enqueue(w)
        }
        if(dist(w) == (dist(v) + 1.0))
        {
          sigma(w) += sigma(v)
          predecessors(w).+=(v)
        }
      }
    }
    
    while (!(s.isEmpty))
    {
      val v = s.pop()
      for (w <- predecessors(v))
      {
        delta(w) += (sigma(w) / sigma(v)) * (delta(v) + 1.0)
      }
      if (v != id)
      {
        medBC.append((v, delta(v)))
      }
    }
    
    medBC.toList
  }
  
  def getNeighbourMap(vlist: List[VertexId], elist: List[(VertexId, VertexId)]): HashMap[VertexId, List[VertexId]] =
  {
    val neighbourList = new HashMap[VertexId, List[VertexId]]()
    
    vlist.foreach { case(v) => 
      {val nlist = (elist.filter{case(e) => ((e._1 == v) || (e._2 == v))})
               .map{case(e) => {if (e._1 == v) e._2 else e._1 }}
       neighbourList.+=((v,nlist.distinct))
      }
    }
    
    neighbourList
  }
  
  def aggregateGraphletsBetweennessScores(vertexKBcGraph: Graph[(VertexId, List[(VertexId, Double)]), Double]): Graph[Double, Double] =
  {
    val DEFAULT_BC = 0.0
    
    val kBCAdditions = 
      vertexKBcGraph
      .vertices
      .flatMap{case(v, (id, listkBC)) => 
                (listkBC.map{case(w, kBC)=>(w, kBC)})}
    
    val verticeskBC =
      kBCAdditions
      .reduceByKey(_ + _)
      
    val kBCGraph = 
      vertexKBcGraph
      .outerJoinVertices(verticeskBC)({case(v_id,(g_id,g_medkBC),kBC) => (kBC.getOrElse(DEFAULT_BC))})
      
    kBCGraph
  }

}


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
        StructField("Total_Paper_Count" , DoubleType,  true),
        StructField("Total_Citations_Achieved" , DoubleType,  true)
        )
    )


case class Researcher(name: String, position: String)

val collaborations = spark.read.option("header",true).schema(collabartionsSchemaExpected).csv("CitationGraphAnalysis/graph-anayltics/cleaned_data/collaborations.csv")
val collaborations_weighted = collaborations.withColumn("weight", (col("Total_Paper_Count")  + (col("Total_Citations_Achieved") * .5))).drop("Total_Paper_Count", "Total_Citations_Achieved")
val collaborations_weighted_rdd:  RDD[Edge[Double]] = collaborations_weighted.rdd.map { row:Row =>
      Edge(row.getAs[VertexId](0), row.getAs[VertexId](1), row.getAs[Double](2))
}
val collaborations_weighted_rdd_other_direction:  RDD[Edge[Double]] = collaborations_weighted.rdd.map { row:Row =>
      Edge(row.getAs[VertexId](1), row.getAs[VertexId](0), row.getAs[Double](2))}
val collaborations_bi_directed = collaborations_weighted_rdd.union(collaborations_weighted_rdd)

// RDD for researchers vertex
val researchers: RDD[(VertexId, Researcher)] = spark.read.option("header",true).schema(researchersSchemaExpected).csv("CitationGraphAnalysis/graph-anayltics/cleaned_data/unique_authors.csv").rdd.map{x:Row => (x.getAs[VertexId](0),Researcher(x.getAs[String](1),  x.getAs[String](2)))}


val undirected_graph: Graph[Researcher, Double] = Graph(researchers, collaborations_bi_directed)
// run K betweens centrality for dcu + external researchers
val kBCGraph = KBetweenness.run(undirected_graph, 200)
kBCGraph.collect.foreach{
    println
}

// compute dcu researcher only graph
println("DCU researcher ony")
val dcu_researhcers_only_vertex = researchers.filter{ case(id, Researcher(name, pos)) => pos != "Unkown"}.map(x => x._1).collect
val dcu_constricted_vertex = undirected_graph.vertices.filter{ case (id:Long, Researcher(name, pos)) => dcu_researhcers_only_vertex.contains(id)}
val dcu_constricted_edges = undirected_graph.edges.filter{ case Edge(srcid:Long, dstid:Long, weight ) => dcu_researhcers_only_vertex.contains(srcid) && dcu_researhcers_only_vertex.contains(dstid)}
val dcu_only_graph = Graph( dcu_constricted_vertex, dcu_constricted_edges)

// run K betweens centrality for dcu researchers graph only
val kBCGraph_dcu_only = KBetweenness.run(dcu_only_graph, 30)
kBCGraph_dcu_only.collect.foreach{
    println
}