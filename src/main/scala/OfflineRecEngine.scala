import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.commons.io.FileUtils;
import java.io.File
import java.io.IOException

object OfflineRecEngine {
    
	type CommunityId = VertexId
    val affSubDir = "affiliation_table"
    val comSubDir = "community_table"
    
    def main(args: Array[String]) {
        
        val dataset = args(0)
        val output = args(1)
        val numIter = args(2).toInt
        println("reading from: " + dataset)
        println("outputting to: " + output)
	println("#iterations: %s".format(numIter))
        try {
            FileUtils.deleteDirectory(new File(output)) }
        catch { case e: IOException => println("nothing to remove")}
        
        val sc = RecUtil.getContext()
        val graph = RecUtil.loadGraph(sc, dataset)
        
        val t0 = System.nanoTime()
        
        val affiliation = computeAffiliationTable(graph, numIter, output + affSubDir)
        computeCommunityTable(affiliation, output + comSubDir)
        
        val t1 = System.nanoTime()
        println("Elapsed time: " + (t1 - t0)/10e8 + " seconds")
    }
    
	/**
	 * Computes and writes to file the community affiliation for each product id.
	 * 
	 * @result A collection of pairs (productId, communityLabel)
	 */
    def computeAffiliationTable(graph: Graph[Int,Int], numIter: Int, outDir: String)
    		: RDD[(VertexId,CommunityId)] = {
    	println("label propagation...")
    	val affiliation = LabelPropagation.run(graph, numIter).vertices
        println("done")
        
        println("writing affiliation table\n")
        affiliation.sortByKey().saveAsTextFile(outDir)
        affiliation
    }
    
    /**
     * Group products of the same affiliation and write the result to file
     * 
     * @result A collection of pairs (communityLabel, productGroup (String))
     */
    def computeCommunityTable(affiliation: RDD[(VertexId,CommunityId)], outDir: String)
    		: RDD[(CommunityId,String)] = {
    	println("grouping community members...")
    	val nbProd = affiliation.count()
    	val communities = affiliation.groupBy(_._2).mapValues(_.map(_._1).take(11).mkString(" "))
    	val nbCom = communities.count()
    	println("done, #communities: %s, avg. size: %s".format(nbCom,nbProd/nbCom));
    	
    	println("writing community table\n")
    	communities.sortByKey().saveAsTextFile(outDir)
    	
    	communities
    }
        
    /**
     * Deprecated.
     * 
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
