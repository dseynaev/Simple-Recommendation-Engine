import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

object GraphTest {
    
    // val dataset = "data/Amazon0505.txt"
    val dataset = "data/mini.txt"
    
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("Simple Recommendation Engine").setMaster("local[2]")
        val sc = new SparkContext(conf)
        Logger.getRootLogger().setLevel(Level.WARN)
        
        val graph = loadGraph(sc, dataset)
        
        val rec = breadthFirstSearch(sc, graph, 10, 0).keys
        
        println("recommendation:")
        println(rec.collect.mkString("\n"))
    }
    
    def loadGraph(sc: SparkContext, path: String) : Graph[Int,Int] = {
        println("loading graph...")
        val graph = GraphLoader.edgeListFile(sc, dataset)
        val n_vertices = graph.vertices.count()
        val n_edges = graph.edges.count()
        println("graph loaded: vertices: %s , edges: %s".format(n_vertices, n_edges))
        return graph
    }
    
    def breadthFirstSearch(sc: SparkContext, graph: Graph[Int,Int],
            maxDist: Int, source: Int ) : RDD[(VertexId,Int)] = {
        println("performing bfs")
        val initGraph = graph.mapVertices((id, _) => if (id==source) 0 else maxDist+1)
        val distGraph = initGraph.pregel(
            maxDist+1,
            maxDist,
            EdgeDirection.Either)(
            vprog,
            sendMsg,
            mergeMsg)
        val res = distGraph.vertices.filter({ case (id, dist) => dist < maxDist+1 })
        res.sortBy({ case (id, dist) => dist }, true)
    }
    
    def vprog(vertexId: VertexId, value: Int, msg: Int): Int = math.min(msg,value)

    def sendMsg(triplet: EdgeTriplet[Int, Int]): Iterator[(VertexId, Int)] = {
        if (triplet.srcAttr + 1 < triplet.dstAttr) {
            println("I am " + triplet.srcId +  " sending to " + triplet.dstId)
            Iterator((triplet.dstId, triplet.srcAttr + 1))
        } else if (triplet.dstAttr + 1 < triplet.srcAttr) {
            println("I am " + triplet.dstId +  " sending to " + triplet.srcId)
            Iterator((triplet.srcId, triplet.dstAttr + 1))
        } else { Iterator.empty }
    }

    def mergeMsg(msg1: Int, msg2: Int): Int = math.min(msg1, msg2)
    
    /*
    def demo(sc: SparkContext) {
        // Create an RDD for the vertices
        val users: RDD[(VertexId, (String, String))] =
          sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                               (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
        // Create an RDD for edges
        val relationships: RDD[Edge[String]] =
          sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                               Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
        // Define a default user in case there are relationship with missing user
        val defaultUser = ("John Doe", "Missing")
        // Build the initial Graph
        val graph = Graph(users, relationships, defaultUser)
        
        // Count all users which are postdocs
        graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
        // Count all the edges where src > dst
        val test = graph.edges.filter(e => e.srcId > e.dstId).count
    }
    */
}