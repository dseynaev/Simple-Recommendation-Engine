import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

object OfflineRecEngine {
    
    val dataset = "data/Amazon0505.txt"
    // val dataset = "data/mini.txt"
    
    def main(args: Array[String]) {
        
        val sc = RecUtil.getContext()
        val graph = RecUtil.loadGraph(sc, dataset)
        
        // execute stuff
        
    }
    
    /**
     * Offline approach
     * Computes the (groupSize) closest vertices for each vertex
     * 
     * @result A graph with at each vertex a list of (groupSize + 1) elements.
     * The vertex itself is also part of the list.
     */
    def grouping(sc: SparkContext, graph: Graph[Int,Int],
            groupSize: Int, maxIter: Int) : Graph[List[VertexId],Int] = {
        println("performing grouping")
        val initGraph = graph.mapVertices((id,_) => List[VertexId](id))
        val groupGraph = initGraph.pregel (
            Set[VertexId](),
            maxIter,
            EdgeDirection.Both ) (
            grpVprog(groupSize+1),
            grpSendMsg(groupSize+1),
            grpMergeMsg )
        groupGraph
    }
    
    def grpVprog(groupSize : Int)(vertexId: VertexId, grp: List[VertexId],
            msg: Set[VertexId]) : List[VertexId] = {
        var newGrp = grp
        var newSize = newGrp.length
        msg.foreach(id => if (!grp.contains(id) && newSize < groupSize) {newGrp ::= id; newSize+=1})
        newGrp
    }
    
    def grpSendMsg(groupSize : Int)(triplet: EdgeTriplet[List[VertexId], Int])
            : Iterator[(VertexId, Set[VertexId])] = {
        if (triplet.srcAttr.length <  groupSize || triplet.dstAttr.length < groupSize) {
            Iterator((triplet.dstId, triplet.srcAttr.toSet[VertexId]),
                 (triplet.srcId, triplet.dstAttr.toSet[VertexId]))
        } else {
            Iterator.empty
        }
    }
    
    def grpMergeMsg(msg1: Set[VertexId], msg2: Set[VertexId])
            : Set[VertexId] = msg1 union msg2

}