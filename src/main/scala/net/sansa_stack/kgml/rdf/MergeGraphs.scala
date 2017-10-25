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
import org.apache.jena.graph.{Node, Node_URI}
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.{PairRDDFunctions, RDD, RDDOperationScope}

import scala.collection.JavaConverters._

/*
    This object is for merging KBs and making unique set of predicates in two KBs
 */
object MergeGraphs {

  def main(args: Array[String]) = {

    //val input1 = "src/main/resources/dbpedia.nt"
    //val input2 = "src/main/resources/yago.nt"

    val input1 = "src/main/resources/dbpediaOnlyAppleobjects.nt"
    val input2 = "src/main/resources/yagoonlyAppleobjects.nt"

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

    //union of all triples
    val unifiedTriplesRDD = triplesRDD.union(triplesRDD2)
    //merging the middle row:  take unique list of the union of predicates
    val unionRelationIDs = (triplesRDD.getPredicates ++ triplesRDD2.getPredicates)
      .map(line => line.toString()).distinct.zipWithUniqueId
    //convert entities and predicates to index of numbers
    val unionEntityIDs = (
      triplesRDD.getSubjects
        ++ triplesRDD.getObjects
        ++ triplesRDD2.getSubjects
        ++ triplesRDD2.getObjects)
      .map(line => line.toString()).distinct.zipWithUniqueId

    //distinct does not work on Node objects and the result of zipWithUniqueId become wrong
    // therefore firstly I convert node to string by .map(line => line.toString())


    this.printGraphInfo(unifiedTriplesRDD, unionEntityIDs, unionRelationIDs)

    var cm = this.createCoordinateMatrix(unifiedTriplesRDD, unionEntityIDs, unionRelationIDs)

    println("matrix rows:" + cm.numRows() + "\n")
    println("matrix cols:" + cm.numCols() + "\n")

    //find their similarity, subjects and predicates. example> barak obama in different KBs
    // find similarites between URI of the same things in differnet  languages  of dbpeida

    //what I propose is modling it with dep neural networks
    // The training part aims to learn the semantic relationships among
    // entities and relations with the negative entities (bad entities),
    // and the goal of the prediction part is giving a triplet
    // score with the vector representations of entities and relations.
    sparkSession.stop
    sparkSession2.stop
  }

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

     (subjectsWithId, predicates ,objectsWithId)
  }

  def getMatrixEntityValue(unionEntityIDs: RDD[(String, Long)], id: Long): String = {
    //reverse id and entity column
    //val iDtoEntityURIs =  unionEntityIDs.map(line => (line._2, line._1) )
    unionEntityIDs.filter(line => line._2 == id).first()._1
  }

  //put two graphs into an 6 column array , 3 String that are a triple and 3 integer that are array index and id relation
  //make two functions for 1: map urI to id, and 2: map id to URI
  // assuming having this two functions, we make matrix of ids
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

  //make two functions for 1: map urI to id, and 2: map id to URI
  // assuming having this two functions, we make matrix of ids
  def createCoordinateMatrix(unifiedTriplesRDD: RDD[graph.Triple], unionEntityIDs: RDD[(String, Long)],
                             unionRelationIDs: RDD[(String, Long)]): CoordinateMatrix = {

    val sextuple = this.createCoordinateMatrixModel(unifiedTriplesRDD, unionEntityIDs, unionRelationIDs)
    var entries = sextuple.map(line => MatrixEntry(line._4, line._6, line._5))
    var numCols, numRows = unionEntityIDs.count()

    new CoordinateMatrix(entries, numRows, numCols)
  }

  def matchPredicatesByWordNet(unionRelationIDs : RDD[(String, Long)]): RDD[(String, Long ,String, Long)] ={
    // take last section of uri strings
    // apply a wordnet measurement on all pairs
    // replace those that are equal

    unionRelationIDs.map(line => line._2 )


  def prunePredicateURI(predicateURI : String) : String = {
    val predicate = predicateURI.split("/").lastOption.toString.toLowerCase
    predicate
  }

}