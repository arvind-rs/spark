
/*
 *  A simple Spark application to count words in a text.
 *  @author: ArvindRS
 *  @date: 06/19/2017
 */


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object WordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local[2]").set("spark.executor.memory","1g"))
    sc.setLogLevel("WARN")
    
    println("Spark context object: "+ sc)
    
    val threshold = 2
    
    val inputText = "The example application is an enhanced version of WordCount, the canonical MapReduce example. In this version of WordCount, the goal is to learn the distribution of letters in the most popular words in a corpus. The application:"
    
    val inputRDD = sc.parallelize(inputText.split(" "))
    
    val tokens = inputRDD.collect()
    println("Tokens: ")
    tokens.foreach(x => print(x + ","))
    
    val wordCounts = inputRDD.map((_,1)).reduceByKey(_ + _)
    println("Word counts")
    wordCounts.foreach(x => println(x))
    
    val filtered = wordCounts.filter(_._2 >= threshold)
    println("Filtered words:")
    filtered.foreach(println)
    
    val charCounts = filtered.flatMap(_._1.toCharArray()).map((_,1)).reduceByKey(_ + _)
    println("Character counts:")
    charCounts.foreach(println)
  }
}