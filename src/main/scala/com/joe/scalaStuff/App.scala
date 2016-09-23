package com.joe.scalaStuff

// Spark imports
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}


/**
 * @author Joe Antonakakis
 */
object App {
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + " " + b)
  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Max Price Finder")
    val sc = new SparkContext(conf)

    // Do work
    MaxPriceFactory.findMaxPrice(sc, args)

  }
}


object MaxPriceFactory {
  // Test code taken from https://goo.gl/tHi6ug
  def findMaxPrice(sc: SparkContext, args: Array[String]) {
    sc.textFile(args(0))
      .map(_.split(","))
      .map(rec => ((rec(0).split("-"))(0).toInt, rec(1).toFloat))
      .reduceByKey((a,b) => Math.max(a,b))
      .saveAsTextFile(args(1))
  }
}
