import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._

object PartBApplication2Question5 {
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
    
    val graph_comp = graph.connectedComponents

    val graph_comp_size:RDD[(Long,Int)] = graph_comp.vertices.map( a => (a._2, 1) )
                .reduceByKey(_+_)
                
    val max_comp_size = graph_comp_size.reduce(cmp)
    println("The largest connected subgraph has size: " + max_comp_size._2)
  }

  def checkCommonWord(a: Array[String], b: Array[String]): Boolean =  {
    val unionSet = a.toSet & b.toSet
    return if (unionSet.size == 0) false else true
  }
  def cmp(a: (Long, Int), b: (Long, Int)): (Long, Int) = {
    return if (a._2 > b._2) a else b
  }

}
