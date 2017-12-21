package net.sansa_stack.kgml.rdf

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by afshin on 05.12.17.
  */

class EntitiesSimilarity(sc : SparkContext) {


  def matchEntitiesByWordNet(Entities1 : RDD[(String)], Entities2 : RDD[(String)]): Array[(String, String, Double)] = {

    println("------------ First 5 Entities in KG1 -------------")
    Entities1.distinct().take(50).foreach(println)

    println("------------ First 5 Entities in KG2 -------------")
    Entities2.distinct().take(50).foreach(println)
    val similarityThreshold = 0.5
    val similarityHandler = new SimilarityHandler(similarityThreshold)

    val JoindEntities = (Entities1.cartesian(Entities2))

    JoindEntities.take(10).foreach(println(_))

    println("Number of entities after join " + JoindEntities.count())

    val similarPairsRdd = JoindEntities.collect().
      map(x => (x._1, x._2, similarityHandler.getLiteralSimilarity(x._1, x._2)))

    val sameEntities = similarPairsRdd.filter(x => x._3 >= similarityThreshold)
    println("Entities with similarity >"+similarityThreshold +" are: "+ sameEntities.length)

    sameEntities.take(sameEntities.length).foreach(println(_))

    sameEntities
  }
}