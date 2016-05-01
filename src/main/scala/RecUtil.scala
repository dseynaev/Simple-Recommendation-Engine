import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

object RecUtil {
    
    /**
     * Initializes the Spark environment
     */
    def getContext() : SparkContext = {
        val conf = new SparkConf().setAppName("Simple Recommendation Engine").setMaster("local[2]")
        val sc = new SparkContext(conf)
        sc.setLogLevel("OFF")
        Logger.getRootLogger().setLevel(Level.OFF)
        
        sc
    }
    
    /**
     * Loads a graph and prints some basic information to the console.
     * 
     * @result A directed graph with 1 as attribute for every node and vertex.
     */
    def loadGraph(sc: SparkContext, path: String) : Graph[Int,Int] = {
        println("loading graph...")
        val graph = GraphLoader.edgeListFile(sc, path)
        val n_vertices = graph.vertices.count()
        val n_edges = graph.edges.count()
        println("graph loaded: vertices: %s , edges: %s".format(n_vertices, n_edges))
        
        graph
    }
  
}