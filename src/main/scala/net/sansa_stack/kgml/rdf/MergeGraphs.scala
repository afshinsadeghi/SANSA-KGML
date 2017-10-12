package net.sansa_stack.kgml.rdf

/**
  * Created by afshin on 10.10.17.
  */

import java.io.ByteArrayInputStream
import java.net.URI

import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDD, JenaSparkRDDOps}
import net.sansa_stack.rdf.spark.model.TripleRDD._
import net.sansa_stack.rdf.spark.model.JenaSparkRDD
import org.apache.jena.graph.Node_URI
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/*
    This object is for merging KBs and making unique set of predicates in two KBs
 */
object MergeGraphs {

  def main(args: Array[String]) = {

    val input1 = "src/main/resources/dbpedia.nt"
    val input2 = "src/main/resources/yago.nt"
    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader example (" + input1 + ")")
      .getOrCreate()

    val sparkSession2 = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple reader example (" + input2 + ")")
      .getOrCreate()

    val triplesRDD = NTripleReader.load(sparkSession, URI.create(input1))

    val triplesRDD2 = NTripleReader.load(sparkSession2, URI.create(input2))

    val unifiedTriplesRDD = triplesRDD.union(triplesRDD2)

    unifiedTriplesRDD.take(50).foreach(println(_))

    val unionRelationIDs = (triplesRDD.getPredicates ++ triplesRDD2.getPredicates).distinct.zipWithUniqueId()


    val unionEntityIDs = (triplesRDD.getSubjects
      ++ triplesRDD.getObjects ++ triplesRDD2.getSubjects
      ++ triplesRDD2.getObjects)
      .distinct
      .zipWithUniqueId()

    //unionEntityIDs.take(5).foreach(println(_))
    //unionRelationIDs.take(5).foreach(println(_))

    def numEntities = unionEntityIDs.count()
    println("Num union Entities "+  numEntities +  " \n")
    def numRelations = unionRelationIDs.count()
    println("Num union Relations "+ numRelations + " \n")

    println("all 615 Relations  \n")
    unionRelationIDs.take(615).foreach(println(_))

    sparkSession.stop
    sparkSession2.stop
    //val mappedTriples =

    //done: put that into a matrix then merge the middle row
    //done: take unique predicates
    //done: convert entities and predicates to index of numbers

    //find their similarity, subjects and predicates. example> barak obama in different KBs
    // find similarites between URI of the same things in differnet  languages  of dbpeida

    //what I propose is modling it with dep neural networks
    // The training part aims to learn the semantic relationships among
    // entities and relations with the negative entities (bad entities),
    // and the goal of the prediction part is giving a triplet
    // score with the vector representations of entities and relations.

  }
}