/**
  * Created by afshin on 10.10.17.
  */

package net.sansa_stack.kgml.rdf

import java.net.URI

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.TripleRDD._
import org.apache.spark.sql.SparkSession

object TestMergeGraphs {
  def main(args: Array[String]): Unit = {

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

    //this.printGraphInfo(unifiedTriplesRDD, unionEntityIDs, unionRelationIDs)


    val mg: MergeKGs = new MergeKGs(sparkSession.sparkContext, triplesRDD, triplesRDD2 )

    // In comparison to number of subjects and objects the number of predicates are very small, that is why we use
    // coordinate matrix format which is good for spars matrices
    var cm = mg.createCoordinateMatrix(unifiedTriplesRDD, unionEntityIDs, unionRelationIDs)

    println("matrix rows:" + cm.numRows() + "\n")
    println("matrix cols:" + cm.numCols() + "\n")

    // gives us the ID of the first object that has relation with subject with id 1
    println("ID of the first object that has relation with subject with id 1"  +
      cm.entries.filter(entry => entry.i == 1 ).first().j + "\n")

    // gives us the ID of the predicate that has relation with subject with id 1 and object with id 172
    println("ID of the predicate that has relation with subject with id 1 and object with id 172" +
      cm.entries.filter(entry => entry.i == 1  &&  entry.j == 172 ).first().value  + "\n")

    var test = mg.matchPredicatesByWordNet(unionRelationIDs)
    test.take(10).foreach(println(_))

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
}