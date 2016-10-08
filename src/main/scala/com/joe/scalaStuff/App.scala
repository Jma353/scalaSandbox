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
    // Spark Configuration
    val conf = new SparkConf().setAppName("My App")
    // SparkContext to drive computation
    val sc = new SparkContext(conf)

    // Do work
    SimpleTransformations.findMovieBefore(sc, args(0).toInt, args(1))

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


object WordCount {

  // Count the words in the provided file
  def wordCount(sc: SparkContext, args: Array[String]): Unit = {

    sc.textFile(args(0))
      .flatMap(line => line.split(",").drop(0))
      .map(word => (word, 1))
      .reduceByKey {case (a, b) => a + b}
    .saveAsTextFile(args(1))
    // Could alternatively collect() and then forEach()

  }

}

// Simple RDD transformations for the purposes of learning
object SimpleTransformations {

  // Movie data we're working with
  val movies = Array("Frozen, 2013", "Toy Story, 1995", "WALL-E, 2008", "Despicable Me, 2010",
    "Shrek, 2001", "The Lego Movie, 2014", "Alice in Wonderland, 2010")

  // Specify a year
  def findMovieBefore(sc: SparkContext, year: Int, outputNamespace: String): Unit = {
    // Filter on that year
    val filteredMovies = sc.parallelize(movies)
      .filter(m => m.split(",").last.trim.toInt < year)

    // Add "old", and "ancient" tags
    val taggedMovies = filteredMovies.map(d => (d, Set("old", "ancient")))

    // Grab the tags from each, and flatMap them (so set of 2 becomes 2 separate RDD's)
    // and call distinct to remove duplicates
    val movieTags = taggedMovies.flatMap { case (d, tags) => tags }.distinct

    // Cartesian allows us to cross records (like a full JOIN in db)
    // Basically keep around the ones where a != b
    val pairs = movieTags.cartesian(movieTags).filter { case (a,b) => a != b }


    // Saving the results as necessary
    filteredMovies.saveAsTextFile(outputNamespace + "_filtered_movies")
    taggedMovies.saveAsTextFile(outputNamespace + "_tagged_movies")
    movieTags.saveAsTextFile(outputNamespace + "_movie_tags")
    pairs.saveAsTextFile(outputNamespace + "_tag_pairs")

  }

}











