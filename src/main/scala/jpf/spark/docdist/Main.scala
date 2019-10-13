package jpf.spark.docdist

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    // set logger level
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    if (!System.getProperty("java.version").startsWith("1.8")) {
      println("The configured version of spark requires java 8 (1.8).")
      println("Using newer/older version may crash the program.")
      sys.exit(1)
    }

    // simple arguments check
    if (args.length != 2) {
      println("Usage: docdist <file1> <file2>")
      sys.exit(2)
    }

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Document Distance")
      .getOrCreate()

    // load file
    try {
      val text1 = spark.sparkContext.textFile(args(0))
      val text2 = spark.sparkContext.textFile(args(1))

      val text1WordsRDD = text1.flatMap(line => line.split("\\s+"))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
      val text2WordsRDD = text2.flatMap(line => line.split("\\s+"))
        .map(word => (word, 1))
        .reduceByKey(_ + _)

      println(s"Number of words in ${args(0)}: ${text1WordsRDD.count()}")
      println(s"Number of words in ${args(1)}: ${text2WordsRDD.count()}")

      val doc1Norm = math.sqrt(text1WordsRDD.values.map(x => x * x).sum())
      val doc2Norm = math.sqrt(text2WordsRDD.values.map(x => x * x).sum())

      println(s"Doc1 norm: $doc1Norm")
      println(s"Doc2 norm: $doc2Norm")

      val innerProduct = text1WordsRDD.union(text2WordsRDD)
        .groupByKey()
        .mapValues(x => if (x.size <= 1) 0.0 else x.product)
        .values.sum()

      println(s"Inner product: $innerProduct")

      val cosineSimilarity = innerProduct / (doc1Norm * doc2Norm)
      println(f"Cosine similarity: $cosineSimilarity%.5f")
    } catch {
      case err: org.apache.hadoop.mapred.InvalidInputException =>
        println(s"Invalid input file: ${err.getMessage}")
        sys.exit(1)
    }

    sys.exit(0)
  }
}