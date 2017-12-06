import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._

object PartBApplication2Question2 {
  def main(args: Array[String]) {
    
    val spark = SparkSession
        .builder
        .appName("PartBApplication2Question2")
        .config("spark.driver.memory", "1g")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "/home/ubuntu/logs")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "4")
        .config("spark.task.cpus", "1")
        .getOrCreate()
    val sc = spark.sparkContext
    
    val file_name = args(0)
    val vfile = sc.textFile(file_name)
    val vRDD: RDD[(VertexId, Array[String])] = vfile.map(line => line.split(","))
            .zipWithIndex()
            .map(_.swap)

    val eRDD:RDD[Edge[(VertexId, VertexId)]] = vRDD.cartesian(vRDD)
                  .filter(x => (x._1._1 != x._2._1) && checkCommonWord(x._1._2, x._2._2) == true)
                  .map(x => Edge(x._1._1, x._2._1))

    val graph = Graph(vRDD, eRDD)
    
    val popular_vertex: VertexRDD[(Int, Array[String])] = graph.aggregateMessages[(Int, Array[String])](
      triplet => { //Map Function
	// send message to source vertex containing 1 and source vertex attr
        triplet.sendToSrc(1, triplet.srcAttr)
      },
      // add number of edges from source
      (a,b) => (a._1 + b._1, a._2)
    )
    val most_popular_vertex = popular_vertex.reduce(cmp)
    println("The most popular vertex ID is: " + most_popular_vertex._1)
    println("The word list of the most popular vertex: ")
    most_popular_vertex._2._2.foreach(println(_))
  }

  def checkCommonWord(a: Array[String], b: Array[String]): Boolean =  {
    val unionSet = a.toSet & b.toSet
    return if (unionSet.size == 0) false else true
  }

  def cmp(a: (VertexId,(Int,Array[String])), b: (VertexId, (Int,Array[String]))): (VertexId, (Int,Array[String])) = {
    if (a._2._1 == b._2._1)
      return if (a._2._2.length > b._2._2.length) a else b
    return if (a._2._1 > b._2._1) a else b
  }
}
