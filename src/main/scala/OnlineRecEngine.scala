import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import scala.collection.immutable.StringOps

object OnlineRecEngine {
    
    def main(args: Array[String]) {
        
        val dataset = args(0)
        val sc = RecUtil.getContext()
        val graph = RecUtil.loadGraph(sc, dataset)
        
        var line = ""
        while (line != null) {
            print("\nenter product id(s): ")
            line = Console.readLine()
            if (line=="exit")
                return
                
            val products = line.split(",").map(_.toInt).toVector
            
            // val rec = breadthFirstSearch(sc, graph, 4, products(0)).tail
            val rec = multiBFS(sc, graph, 4, products).filter({case (id,_) => !products.contains(id)})
            print("recommendation:")
            
            println(rec.take(10).map(v => v._1).mkString(", "))
        }
    }
    
    /**
     * Online approach
     * Generalization of SSSP for more points.
     * 
     * @result A collection (RDD) of (vertex,distance) tuples, sorted by their
     * aggregated distances from the source vertices.
     */
    def multiBFS(sc: SparkContext, graph: Graph[Int,Int],
            maxDist: Int, sources: Vector[Int] ) : RDD[(VertexId,Int)] = {
        println("performing multi-bfs")
        val initGraph = graph.mapVertices((id,_) => sources.map(s => if (id==s) 0 else maxDist+1))
        val distGraph = initGraph.pregel(
            sources.map(_ => maxDist+1),
            maxDist,
            EdgeDirection.Either)(
            mbfsVprog,
            mbfsSendMsg,
            mbfsMergeMsg)
        val res = distGraph.vertices.mapValues { distVec: Vector[Int] => distVec.reduce(_ + _) }
        val res_f = res.filter( { case (id, score) => score < (maxDist + 1) * sources.length } )
        res_f.sortBy( { case (_,dist) => dist }, true)
    }
            
    def mbfsVprog(vertexId: VertexId, value: Vector[Int], msg: Vector[Int])
            : Vector[Int] = mbfsMergeMsg(value,msg)
    
    def mbfsSendMsg(triplet: EdgeTriplet[Vector[Int], Int]) : Iterator[(VertexId,Vector[Int])] = {
        if (sendCond(triplet.srcAttr,triplet.dstAttr)) {
            Iterator((triplet.dstId, triplet.srcAttr.map(_ + 1)))
        } else if (sendCond(triplet.dstAttr,triplet.srcAttr)) {
            Iterator((triplet.srcId, triplet.dstAttr.map(_ + 1)))
        } else {
            Iterator.empty
        }
    }
    
    def sendCond(val1: Vector[Int], val2: Vector[Int]) : Boolean
        = (val1,val2).zipped.map((x,y) => x + 1 < y).reduce(_ || _)
    
    def mbfsMergeMsg(msg1 : Vector[Int], msg2 : Vector[Int]) : Vector[Int]
        = (msg1,msg2).zipped.map(math.min)
    
    /**
     * Online approach
     * Performs a breadth-first search starting from the given source vertex.
     * 
     * @result A collection (RDD) of (vertex,distance) tuples, sorted by their
     * distance from the source vertex.
     */
    def breadthFirstSearch(sc: SparkContext, graph: Graph[Int,Int],
            maxDist: Int, source: Int ) : RDD[(VertexId,Int)] = {
        println("performing bfs")
        val initGraph = graph.mapVertices((id, _) => if (id==source) 0 else maxDist+1)
        val distGraph = initGraph.pregel(
            maxDist+1,
            maxDist,
            EdgeDirection.Either)(
            bfsVprog,
            bfsSendMsg,
            bfsMergeMsg)
        val res = distGraph.vertices.filter({ case (id, dist) => dist < maxDist+1 })
        res.sortBy({ case (id, dist) => dist }, true)
    }
    
    def bfsVprog(vertexId: VertexId, value: Int, msg: Int): Int = math.min(msg,value)

    def bfsSendMsg(triplet: EdgeTriplet[Int, Int]): Iterator[(VertexId, Int)] = {
        if (triplet.srcAttr + 1 < triplet.dstAttr) {
            // println("I am " + triplet.srcId +  " sending to " + triplet.dstId)
            Iterator((triplet.dstId, triplet.srcAttr + 1))
        } else if (triplet.dstAttr + 1 < triplet.srcAttr) {
            // println("I am " + triplet.dstId +  " sending to " + triplet.srcId)
            Iterator((triplet.srcId, triplet.dstAttr + 1))
        } else { Iterator.empty }
    }

    def bfsMergeMsg(msg1: Int, msg2: Int): Int = math.min(msg1, msg2)
}