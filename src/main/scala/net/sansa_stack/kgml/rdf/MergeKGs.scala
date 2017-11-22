package net.sansa_stack.kgml.rdf

/**
  * Created by afshin on 10.10.17.
  */

import java.io.ByteArrayInputStream
import java.net.URI

import com.sun.rowset.internal.Row

import scala.collection.{immutable, mutable}
import org.apache.spark.sql.{Row, SparkSession}
import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDD, JenaSparkRDDOps}
import net.sansa_stack.rdf.spark.model.TripleRDD._
import net.sansa_stack.rdf.spark.model.JenaSparkRDD
import org.aksw.jena_sparql_api.utils.Triples
import org.apache.jena.graph
import org.apache.jena.graph.{Node, Node_URI, Triple}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.{PairRDDFunctions, RDD, RDDOperationScope}


/*
    This object is for merging KBs and making unique set of predicates in two KBs
 */
 class MergeKGs(cs : SparkContext,triplesRDD : RDD[org.apache.jena.graph.Triple], triplesRDD2 : RDD[Triple] ) {


  /**
    * A function to print out information related to a merged KG
    * @param unifiedTriplesRDD
    * @param unionEntityIDs
    * @param unionRelationIDs
    */
  def printGraphInfo(unifiedTriplesRDD: RDD[graph.Triple], unionEntityIDs: RDD[(String, Long)],
                     unionRelationIDs: RDD[(String, Long)]): Unit = {
    def numEntities = unionEntityIDs.count()

    println("Num union Entities " + numEntities + " \n")

    def numRelations = unionRelationIDs.count()

    println("Num union Relations " + numRelations + " \n")


    println("10 first Relations  \n")
    unionRelationIDs.take(10).foreach(println(_))

    println("10 first triples of union RDD  \n")
    unifiedTriplesRDD.take(10).foreach(println(_))
  }

  /**
    * Creates a model of two KGs into an 6 column array , subjects(in string) With Id(Long), predicates and
    * objects(string )With Id(Long)
    * @param unifiedTriplesRDD
    * @param unionEntityIDs
    * @param unionRelationIDs
    * @return
    */
    def create3ColumnModel(unifiedTriplesRDD: RDD[graph.Triple], unionEntityIDs: RDD[(String, Long)],
                         unionRelationIDs: RDD[(String, Long)]): (RDD[(Node, Long)], RDD[(Node, Long)], RDD[(Node, Long)]) = {

    var subjects = unifiedTriplesRDD.map(line => (line.getSubject.toString, line.getSubject))
    var subjectsWithId = subjects.join(unionEntityIDs).map(line => line._2)

    var predicates = unifiedTriplesRDD.map(line => (line.getPredicate.toString, line.getPredicate))
      .join(unionRelationIDs).map(line => line._2)
    var objects = unifiedTriplesRDD.map(line => (line.getObject.toString, line.getObject))
    var objectsWithId = objects.join(unionEntityIDs).map(line => line._2)

    println("10 subjects  \n")
    subjectsWithId.take(10).foreach(println(_))
    println("10 predicates  \n")
    predicates.take(10).foreach(println(_))
    println("10 objects  \n")
    objectsWithId.take(10).foreach(println(_))

     (subjectsWithId, predicates , objectsWithId)
  }

  def getMatrixEntityValue(unionEntityIDs: RDD[(String, Long)], id: Long): String = {
    //reverse id and entity column
    //val iDtoEntityURIs =  unionEntityIDs.map(line => (line._2, line._1) )
    unionEntityIDs.filter(line => line._2 == id).first()._1
  }

  /**
    * Creates a model of two KGs into an 6 column array , 3 String that are a triple and 3 integer that
    * are array index and id relation make two functions for
    * 1: map urI to id, and
    * 2: map id to URI assuming having this two functions, we make matrix of ids
    * @param unifiedTriplesRDD
    * @param unionEntityIDs
    * @param unionRelationIDs
    * @return
    */
  def createCoordinateMatrixModel(unifiedTriplesRDD: RDD[graph.Triple], unionEntityIDs: RDD[(String, Long)],
                                  unionRelationIDs: RDD[(String, Long)]): RDD[(String, String, String, Long, Long, Long)] = {

    //unionEntityIDs.take(5).foreach(println(_))
    //unionRelationIDs.take(5).foreach(println(_))

    var unionEntityIDsInString = unionEntityIDs.map(line => (line._1, line._2))

    //var triples = unifiedTriplesRDD.zipWithIndex()
    var triples2 = unifiedTriplesRDD.map(line =>
      (line.getSubject.toString, (line.getSubject.toString, line.getPredicate.toString, line.getObject.toString)))

    var quadruple = triples2.join(unionEntityIDs).map(line => line._2)

    var quintuple = quadruple.map(line => (line._1._2, (line._1._1, line._1._2, line._1._3, line._2)))
      .join(unionRelationIDs).map(line => line._2)

    val sextuple = quintuple.map(line => (line._1._3, (line._1._1, line._1._2, line._1._3, line._1._4, line._2)))
      .join(unionEntityIDs).map(line => line._2)
      .map(line => (line._1._1, line._1._2, line._1._3, line._1._4, line._1._5, line._2))

    //sextuple.take(100).foreach(println(_))
    sextuple
  }

  /**
    * Creates a matrix OF IDS from unfied triples of two KGs
    * To use this this matrix functions are needed
    *  1: map urI to id, and 2: map id to URI
    *
    * @param unifiedTriplesRDD
    * @param unionEntityIDs
    * @param unionRelationIDs
    * @return
    */
  def createCoordinateMatrix(unifiedTriplesRDD: RDD[graph.Triple], unionEntityIDs: RDD[(String, Long)],
                             unionRelationIDs: RDD[(String, Long)]): CoordinateMatrix = {

    val sextuple = this.createCoordinateMatrixModel(unifiedTriplesRDD, unionEntityIDs, unionRelationIDs)
    var entries = sextuple.map(line => MatrixEntry(line._4, line._6, line._5))
    var numCols, numRows = unionEntityIDs.count()

    new CoordinateMatrix(entries, numRows, numCols)
  }


  /**
    *
    * @param unionRelationIDs
    * @return
    */
  def matchPredicatesByWordNet(unionRelationIDs : RDD[(String, Long)]): RDD[(String, Long, String, Long)] = {
    // take last section of uri strings
    // apply a WordNet measurement on all pairs
    // replace those that are equal

   // var editedPredica
   // tes = unionRelationIDs.map(line => (line._1.split("/").last.toLowerCase,
     // line._2, line._1, line._2))
    val similarityHandler = new SimilarityHandler(0.7)

    var editedPredicates = unionRelationIDs.map(line => (line._1.split("/").last.toLowerCase, line._2))


    val predicateBC =  cs.broadcast(editedPredicates.collect())
    val similarPairsRdd = editedPredicates.flatMap(x =>
      predicateBC.value.filter(y => similarityHandler.arePredicatesEqual(x._1,y._1))
        .map(y => (x,y)))
    similarPairsRdd.take(10).foreach(println(_))


    val similarPairsRddTest = editedPredicates.map(line => (line._1, line._2, line._1, line._2))

  similarPairsRddTest
  }

  val pruneURI = (uri : String) => uri.split("/").last.toLowerCase


}