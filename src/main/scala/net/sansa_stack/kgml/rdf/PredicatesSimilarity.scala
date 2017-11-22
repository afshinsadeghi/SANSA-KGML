package net.sansa_stack.kgml.rdf

import org.apache.spark.rdd.RDD
import org.apache.jena.graph
import org.apache.jena.graph.{Node, Node_URI, Triple}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.{PairRDDFunctions, RDD, RDDOperationScope}

//class PredicatesSimilarity(cs : SparkContext,triplesRDD : RDD[org.apache.jena.graph.Triple], triplesRDD2 : RDD[Triple] ) {
class PredicatesSimilarity(sc : SparkContext) {
  //var similarPairsRdd:Array[(String, String, Double)] = null

  /*def matchPredicatesByWordNet(unionRelationIDs : RDD[(String, Long)]): RDD[(String, Long, String, Long)] = {
    // take last section of uri strings
    // apply a WordNet measurement on all pairs
    // replace those that are equal

    // var editedPredica
    // tes = unionRelationIDs.map(line => (line._1.split("/").last.toLowerCase,
    // line._2, line._1, line._2))
    val similarityHandler = new SimilarityHandler(0.7)

    var editedPredicates = unionRelationIDs.map(line => (line._1.split("/").last.toLowerCase, line._2))


    val predicateBC =  cs.broadcast(editedPredicates.collect())
    val similarPairsRdd = editedPredicates.flatMap(x =>predicateBC.value.filter(y => similarityHandler.arePredicatesEqual(x._1,y._1))
        .map(y => (x,y)))
    similarPairsRdd.take(10).foreach(println(_))


    val similarPairsRddTest = editedPredicates.map(line => (line._1, line._2, line._1, line._2))

    similarPairsRddTest
  }*/
//compare two RDD of string and return a new RDD with the two string and the similarity value RDD[string,string,value]
  //def matchPredicatesByWordNet(Predicates1 : RDD[(String, Long)], Predicates2 : RDD[(String, Long)]) = {
  def matchPredicatesByWordNet(Predicates1 : RDD[(String)], Predicates2 : RDD[(String)]) = {
    println("first 5 predicates in KG1 ")
    Predicates1.distinct().take(5).foreach(println)
    //println("first predicate "+ Predicates1.take(Predicates1.count().toInt).apply(0))
    println("first 5 predicates in KG2 ")
    Predicates2.distinct().take(5).foreach(println)
    //println("Second predicate "+ Predicates2.first())
    //println("first predicate "+ predicatesWithoutURIs2.take(predicatesWithoutURIs2.count().toInt).apply(1))
    val similarityHandler = new SimilarityHandler(0.7)

    println("##########")

    /*val pairsPredicates1 = Predicates1.zipWithIndex.map((x) =>(x._2, x._1))
    println("Number of predicates in first KG " + pairsPredicates1.count()) //313
    pairsPredicates1.take(10).foreach(println(_))

    val pairsPredicates2 = Predicates2.zipWithIndex.map((x) =>(x._2, x._1))
    println("Number of predicates in second KG " + pairsPredicates2.count()) //81
    pairsPredicates2.take(10).foreach(println(_))*/

    val JoindPredicates = (Predicates1.cartesian(Predicates2)).collect()
    JoindPredicates.take(10).foreach(println(_))
    println("Number of predicates after join " + JoindPredicates.length) //25353

    val similarPairsRdd = JoindPredicates.map(x => (x._1, x._2, similarityHandler.getMeanWordNetNounSimilarity(x._1, x._2)))
    similarPairsRdd.take(10).foreach(println(_))


  }

}
