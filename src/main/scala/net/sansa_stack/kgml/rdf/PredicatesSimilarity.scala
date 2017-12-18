package net.sansa_stack.kgml.rdf

import org.apache.spark.rdd.RDD
import org.apache.jena.graph
import org.apache.jena.graph.{Node, Node_URI, Triple}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.{PairRDDFunctions, RDD, RDDOperationScope}
/*
* Created by Shimaa
*
* */

class PredicatesSimilarity(sc : SparkContext) extends Serializable{
//compare two RDD of string and return a new RDD with the two string and the similarity value RDD[string,string,value]
  def matchPredicatesByWordNet(Predicates1 : RDD[(String)], Predicates2 : RDD[(String)]):
  RDD[(String, String, Double)] = {
   println("Number of predicates in KG1 "+ Predicates1.count())
    println("Number of predicates in KG2 "+ Predicates2.count())
    //Predicates1.distinct().take(5).foreach(println)
    //println("first predicate "+ Predicates1.take(Predicates1.count().toInt).apply(0))
    //println("first 5 predicates in KG2 ")
    //Predicates2.distinct().take(5).foreach(println)
    //println("Second predicate "+ Predicates2.first())
    //println("first predicate "+ predicatesWithoutURIs2.take(predicatesWithoutURIs2.count().toInt).apply(1))

    val similarityThreshold = 0.8
    val similarityHandler = new SimilarityHandler(similarityThreshold)

    println("##########")

    /*val pairsPredicates1 = Predicates1.zipWithIndex.map((x) =>(x._2, x._1))
    println("Number of predicates in first KG " + pairsPredicates1.count()) //313
    pairsPredicates1.take(10).foreach(println(_))

    val pairsPredicates2 = Predicates2.zipWithIndex.map((x) =>(x._2, x._1))
    println("Number of predicates in second KG " + pairsPredicates2.count()) //81
    pairsPredicates2.take(10).foreach(println(_))*/

    val JoindPredicates: RDD[(String,String)] = Predicates1.cartesian(Predicates2)
    //JoindPredicates.take(10).foreach(println(_))
    println("Number of paird predicates after join " + JoindPredicates.count()) //25353



    //var s = new Array[Double](JoindPredicates.count().toInt)
//    for( i <- 0 to (JoindPredicates.count()-1).toInt){
//      s(i.toInt) = similarityHandler.getPredicateSimilarity(JoindPredicates.take(JoindPredicates.count().toInt).apply(i.toInt)._1,
//        JoindPredicates.take(JoindPredicates.count().toInt).apply(i.toInt)._2)
//    }


   // s.take(10).foreach(println(_))


    JoindPredicates.cache()
    val similarPairsRdd = JoindPredicates.map(x => (x._1, x._2, similarityHandler.
      jaccardPredicateSimilarityWithWordNet(x._1, x._2)))

   // val similarPairsRdd = JoindPredicates.collect().map(x => (x._1, x._2, similarityHandler.
   //   jaccardPredicateSimilarityWithWordNet(x._1, x._2)))
    //println("Similarity between paird predicates = ")
    //similarPairsRdd.take(10).foreach(println(_))


  //get predicates with similarity >= 0.5
   val samePredicates = similarPairsRdd.filter(x => x._3 >= similarityThreshold)
    println("Predicates with similarity >= "+ similarityThreshold + " are: "+ samePredicates.count()) //64
//    samePredicates.take(samePredicates.length).foreach(println(_)) //prints all the predicates

   samePredicates
  }

}
