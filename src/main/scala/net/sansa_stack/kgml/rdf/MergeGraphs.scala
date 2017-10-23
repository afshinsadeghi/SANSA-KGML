package net.sansa_stack.kgml.rdf

/**
  * Created by afshin on 10.10.17.
  */

import java.io.ByteArrayInputStream
import java.net.URI

import com.sun.rowset.internal.Row

import scala.collection.mutable
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
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions

/*
    This object is for merging KBs and making unique set of predicates in two KBs
 */
object MergeGraphs {

  def main(args: Array[String]) = {

    val input1 = "src/main/resources/dbpedia.nt"
    val input2 = "src/main/resources/yago.nt"

    //val input1 = "src/main/resources/dbpediaOnlyAppleobjects.nt"
    //val input2 = "src/main/resources/yagoonlyAppleobjects.nt"
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


    val unionRelationIDs = (triplesRDD.getPredicates ++ triplesRDD2.getPredicates).distinct.zipWithUniqueId()


    val unionEntityIDs = (triplesRDD.getSubjects
      ++ triplesRDD.getObjects ++ triplesRDD2.getSubjects
      ++ triplesRDD2.getObjects)
      .distinct
      .zipWithUniqueId()


    //val mappedTriples =



   this.printGraphInfo(unifiedTriplesRDD, unionEntityIDs, unionRelationIDs)

   this.creatMatrixModel(unifiedTriplesRDD, unionEntityIDs, unionRelationIDs)


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

  def printGraphInfo(unifiedTriplesRDD: RDD[graph.Triple], unionEntityIDs: RDD[(Node, Long)], unionRelationIDs: RDD[(Node, Long)]) ={
    def numEntities = unionEntityIDs.count()
    println("Num union Entities "+  numEntities +  " \n")
    def numRelations = unionRelationIDs.count()
    println("Num union Relations "+ numRelations + " \n")


    println("10 first Relations  \n")
    unionRelationIDs.take(10).foreach(println(_))

    println("10 first triples of union RDD  \n")
    unifiedTriplesRDD.take(10).foreach(println(_))

  }

  //make two functions for 1: map urI to id, and 2: map id to URI
  // assuming having this two functions, we make matrix of ids
  def creatMatrixModel(unifiedTriplesRDD: RDD[graph.Triple], unionEntityIDs: RDD[(Node, Long)], unionRelationIDs: RDD[(Node, Long)]) = {

    //unionEntityIDs.take(5).foreach(println(_))
    //unionRelationIDs.take(5).foreach(println(_))

    //reverse id and entity column
    val iDtoEntityURIs =  unionEntityIDs.map(line => (line._2, line._1) )
    //iDtoEntityURIs.take(5).foreach(println(_))

    //var subjects = unifiedTriplesRDD.map(line => line.getSubject)
    var subjects = unifiedTriplesRDD.map(line => (line.getSubject,line.getSubject) )

    var subjectWithIds = subjects.join(unionEntityIDs).map(line => line._2)

    subjectWithIds.take(5).foreach(println(_))



    var predicates = unifiedTriplesRDD.map(line => ( line.getPredicate, line.getPredicate)).join(unionRelationIDs).map(line => line._2)
    var objects = unifiedTriplesRDD.map(line => (line.getObject, line.getObject)).join(unionEntityIDs).map(line => line._2)


    predicates.take(5).foreach(println(_))
    objects.foreach(println(_))
    /*
        val entries: RDD[MatrixEntry] = unifiedTriplesRDD.map(triple =>  triple.getObject.)  // an RDD of matrix entries
        // Create a CoordinateMatrix from an RDD[MatrixEntry].
        val mat: CoordinateMatrix = new CoordinateMatrix(entries)

        // Get its size.
        val m = mat.numRows()
        val n = mat.numCols()

        // Convert it to an IndexRowMatrix whose rows are sparse vectors.
        val indexedRowMatrix = mat.toIndexedRowMatrix()

    */
    //done: put that into a matrix then merge the middle row
    //done: take unique predicates
    //done: convert entities and predicates to index of numbers
  }

}