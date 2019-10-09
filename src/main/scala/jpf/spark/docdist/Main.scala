package jpf.spark.docdist

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Word count")
      .getOrCreate()

    val data = spark.sparkContext.parallelize(
      Seq("I like Spark", "Spark is awesome",
        "My first Spark job is working now and is counting these words"))

    val wordCounts = data
      .flatMap(row => row.split(" ")).map(word => (word, 1))
      .reduceByKey(_ + _)
    wordCounts.foreach(println)
  }
}