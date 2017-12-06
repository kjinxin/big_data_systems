import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import org.apache.spark.graphx.PartitionStrategy._

object PartBApplication2Question4 {
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
    val vRDD: RDD[(VertexId, String)] = vfile.map(line => line)
            .zipWithIndex()
            .map(_.swap)


    val flat_words = vRDD.flatMap{ case (id, words) => 
      val wordlist = words.split(",")
      wordlist.map(word => (word, 1))
    }
    val most_pop_word = flat_words.reduceByKey(_+_).reduce(cmp)
	
    println("The most popular word: ")
    println(most_pop_word)
  }
  def cmp(a: (String, Int), b: (String, Int)): (String, Int) = {
    return if (a._2 > b._2) a else b
  }
}
