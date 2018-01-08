package net.sansa_stack.kgml.rdf

/**
  * Created by afshin on 10.10.17.
  */

import java.io.{ByteArrayInputStream, File, PrintWriter}
import java.net.URI

import breeze.linalg.max
import com.sun.rowset.internal.Row
import net.didion.jwnl.data.POS
import net.sansa_stack.kgml.rdf.wordnet.WordNet

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
import net.didion.jwnl.data.POS

import scala.collection.JavaConverters._

/*
    This object is for merging KBs and making unique set of predicates in two KBs
 */
object Main {

  var input1 = ""
  var input2 = ""
  var input3 = ""
  var input4 = "false"
  def main(args: Array[String]) = {
    println("Comparing knowledge graphs")

    if (args.headOption.isDefined) {
      input1 = args(0)
      input2 = args(1)
      input3 = args(2)
      input4 = args(3)
    } else {
      println("Please give pathes to the two files that contains the dataset in N-Triples format and the experiment number")
      println("Otherwise it reads from default example data sets.")
      println("Experiment number 0: third ar General info of the two datasets.")
      println("Experiment number 1: Getting similarity between predicates")
      println("Experiment number 2: Getting similarity between literal entities")
      println("Experiment number 3: Getting similarity between non-literal (called source in jena definition) entities (Subjects and objects)")
      println("Input 4 sets the WordNet similarity to produce RDD if be true and to array if be false")
      println("the default value is false ")
      println(" ")

      println("There is no given argument. For example you can give:")
      input1 = "datasets/dbpediaOnlyAppleobjects.nt"
      input2 = "datasets/yagoonlyAppleobjects.nt"
      input3 = "1"
      input4 = "false"
    }
    println(input1)
    println(input2)
    println(input3)
    println(input4)
    var RunWordNetWithRDD = false
    if(input4 == "false" ){
      RunWordNetWithRDD = false
    } else{
      RunWordNetWithRDD = true
    }
    // val input2 = "datasets/dbpediamapping5k.nt"  //dbpedia-3-9-mappingbased_properties_en
    //  val input1 = "datasets/yagofact5k.nt"


    //val input1 = "datasets/dbpedia.nt"
    //val input2 = "datasets/yago.nt"

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024")
      .config("spark.kryo.registrator", "net.sansa_stack.kgml.rdf.Registrator")
      .appName("Triple merger of " + input1 + " and " + input2 + " ")
      .getOrCreate()

    //    val sparkSession = SparkSession.builder()
    //      .config("spark.kryo.registrator", "net.sansa_stack.owl.spark.dataset.UnmodifiableCollectionKryoRegistrator")
    //      .appName("ManchesterSyntaxOWLAxiomsDatasetBuilderTest").master("local[*]").getOrCreate()

    val triplesRDD1 = NTripleReader.load(sparkSession, URI.create(input1)) // RDD[Triple]
    val triplesRDD2 = NTripleReader.load(sparkSession, URI.create(input2))

    var makeResult0 = false
    var makeResult1 = false
    var makeResult2 = false
    var makeResult3 = false

    if(input3 == "0"){   // General info of the two datasets
      makeResult0 = true
    }
    if(input3 == "1"){ // Getting similarity between predicates
      makeResult1 = true
    }
    if(input3 == "2"){ // Getting similarity between literal entities
      makeResult2 = true
    }
    if(input3 == "3"){ // Getting similarity between non-literal (called source in jena definition) entities (Subjects and objects)
      makeResult3 = true
    }


    if (makeResult0) {
      //##################### Get graph specs ############################
      triplesRDD1.cache()
      println("Number of triples in the first KG file " + input1 + "\n") //dbpedia 2457
      println(triplesRDD1.count().toString)

      println("Number of triples in the second KG " + input2 + "\n") //yago 738
      println(triplesRDD2.count().toString)


      val subject1 = triplesRDD1.map(f => (f.getSubject, 1))
      val subject1Count = subject1.reduceByKey(_ + _)
      println("SubjectCount in the first KG \n")
      subject1Count.take(5).foreach(println(_))

      val predicates1 = triplesRDD1.map(_.getPredicate).distinct
      println("Number of unique predicates in the first KB is " + predicates1.count()) //333
      val predicates2 = triplesRDD2.map(_.getPredicate).distinct
      println("Number of unique predicates in the second KB is " + predicates2.count()) //83

      // Storing the unique predicates in separated files, one for DBpedia and one for YAGO
      val predicatesKG1 = triplesRDD1.map(f => (f.getPredicate, 1))
      val predicate1Count = predicatesKG1.reduceByKey(_ + _)
      //predicates1.take(5).foreach(println(_))
      //val writer1 = new PrintWriter(new File("src/main/resources/DBpedia_Predicates.nt" ))
      //predicate1Count.collect().sortBy(f=> f._2  * -1 ).foreach(x=>{writer1.write(x.toString()+ "\n")})
      //writer1.close()


      val predicatesKG2 = triplesRDD2.map(f => (f.getPredicate, 1))
      val predicate2Count = predicatesKG2.reduceByKey(_ + _)
      //val writer2 = new PrintWriter(new File("src/main/resources/YAGO_Predicates.nt" ))
      //predicate2Count.collect().sortBy(f=> f._2  * -1 ).foreach(x=>{writer2.write(x.toString()+ "\n")})
      //writer2.close()
      //######################### Creating union for the two RDD triples##############################
      //union of all triples
      val unifiedTriplesRDD = triplesRDD1.union(triplesRDD2)

      //union all predicates1
      val unionRelation = triplesRDD1.getPredicates.union(triplesRDD2.getPredicates).distinct()
      println("Number of unique predicates1 in the two KGs is " + unionRelation.count()) //=413 it should be 333+83=416 which means we have 3 predicates1 is the same
      println("10 first unique relations  \n")
      unionRelation.take(10).foreach(println(_))

      val unionRelationIDsssss = (triplesRDD1.getPredicates.union(triplesRDD2.getPredicates)).distinct().zipWithUniqueId
      //val unionRelationIDs = (triplesRDD1.getPredicates ++ triplesRDD2.getPredicates).distinct()
      println("Number of unique relationIDssssss in the two KGs is " + unionRelationIDsssss.count()) //413
      println("10 first unique relationIDssssss  \n")
      unionRelationIDsssss.take(10).foreach(println(_))

      //merging the middle row:  take unique list of the union of predicates1
      //IDs converted to string
      val unionRelationIDs = (triplesRDD1.getPredicates ++ triplesRDD2.getPredicates)
        .map(line => line.toString()).distinct.zipWithUniqueId
      //val unionRelationIDs = (triplesRDD1.getPredicates ++ triplesRDD2.getPredicates).distinct()
      println("Number of unique relationIDs in the two KGs is " + unionRelationIDs.count()) //413
      println("10 first unique relationIDs  \n")
      unionRelationIDs.take(10).foreach(println(_))

      //val unionEntities = triplesRDD1.getSubjects.union(triplesRDD2.getSubjects).union(triplesRDD1.getObjects.union(triplesRDD2.getObjects)).distinct()
      val unionEntities = (triplesRDD1.getSubjects ++ triplesRDD1.getObjects ++ triplesRDD2.getSubjects ++ triplesRDD2.getObjects).distinct().zipWithUniqueId
      println("Number of unique entities in the two KGs is " + unionEntities.count()) //=355
      //println("10 first unique entities  \n")
      //unionEntities.take(10).foreach(println(_))

      //convert entities and predicates1 to index of numbers
      val unionEntityIDs = (
        triplesRDD1.getSubjects
          ++ triplesRDD1.getObjects
          ++ triplesRDD2.getSubjects
          ++ triplesRDD2.getObjects)
        .map(line => line.toString()).distinct.zipWithUniqueId
      println("Number of unique entity IDs in the two KGs is " + unionEntityIDs.count()) //=225!!

    }
    //distinct does not work on Node objects and the result of zipWithUniqueId become wrong
    // therefore firstly I convert node to string by .map(line => line.toString())


    //this.printGraphInfo(unifiedTriplesRDD, unionEntityIDs, unionRelationIDs)
    //var merg = new MergeKGs()

    // var cm = merg.createCoordinateMatrix(unifiedTriplesRDD, unionEntityIDs, unionRelationIDs)

    //println("matrix rows:" + cm.numRows() + "\n")
    //println("matrix cols:" + cm.numCols() + "\n")

    //find their similarity, subjects and predicates1. example> barak obama in different KBs
    // find similarites between URI of the same things in differnet  languages  of dbpeida

    //what I propose is modling it with dep neural networks
    // The training part aims to learn the semantic relationships among
    // entities and relations with the negative entities (bad entities),
    // and the goal of the prediction part is giving i triplet
    // score with the vector representations of entities and relations.

    if (makeResult1) {

      //Getting the predicates without URIs
      val predicatesWithoutURIs1 = triplesRDD1.map(_.getPredicate.getLocalName).distinct() //.zipWithIndex()
      println("Predicates without URI in KG1 are " + predicatesWithoutURIs1.count()) //313
      predicatesWithoutURIs1.distinct().take(5).foreach(println)
      println("first predicate " + predicatesWithoutURIs1.take(predicatesWithoutURIs1.count().toInt).apply(0))

      val predicatesWithoutURIs2 = triplesRDD2.map(_.getPredicate.getLocalName).distinct() //.zipWithIndex()
      println("Predicates without URI in KG2 are " + predicatesWithoutURIs2.count()) //81
      predicatesWithoutURIs2.distinct().take(5).foreach(println)
      println("first predicate " + predicatesWithoutURIs2.first())
      //println("first predicate "+ predicatesWithoutURIs2.take(predicatesWithoutURIs2.count().toInt).apply(1))


      //############################ Getting similarity between predicates ####################################
      println("//############################ Getting similarity between predicates ####################################")
      var preSim = new PredicatesSimilarity(sparkSession.sparkContext)
      var eval: Evaluation = new Evaluation()

      if (!RunWordNetWithRDD) {
        //this creates array:
        val similarPredicates = preSim.matchPredicatesByWordNet(predicatesWithoutURIs1, predicatesWithoutURIs2)
        // this works with array similarPredicates:
        var compresionRatio = eval.compressionRatio(predicatesWithoutURIs1.count() + predicatesWithoutURIs2.count(), similarPredicates.length)
        println("Compression Ration in predicates merging = " + compresionRatio + "%")
      } else {
        val similarPredicates = preSim.matchPredicatesByWordNetRDD(predicatesWithoutURIs1,predicatesWithoutURIs2)
        var compresionRatio = eval.compressionRatio(predicatesWithoutURIs1.count()+predicatesWithoutURIs2.count(),similarPredicates.count())
        println("Compression Ration in predicates merging = " + compresionRatio + "%")
      }
    }

    if (makeResult2) {
      println("//############################ Getting similarity between literal entities  ####################################")

      var preSim = new PredicatesSimilarity(sparkSession.sparkContext)

      val literalObjects1 = triplesRDD1.filter(_.getObject.isLiteral).map(_.getObject.getLiteralValue.toString).distinct()
      val literalObjects2 = triplesRDD2.filter(_.getObject.isLiteral).map(_.getObject.getLiteralValue.toString).distinct()


      println("Literal entities (without URI) in KG1 are " + literalObjects1.count())
      literalObjects1.distinct().take(5).foreach(println)
      println("First literal entity " + literalObjects1.take(literalObjects1.count().toInt).apply(0))

      println("Literal entities (without URI) in KG2 are " + literalObjects2.count()) //81
      literalObjects2.distinct().take(5).foreach(println)
      println("First literal entity" + literalObjects2.first())

      val entSim = new EntitiesSimilarity(sparkSession.sparkContext)
      var eval: Evaluation = new Evaluation()
      if (!RunWordNetWithRDD) {
        //this creates array:
        val similarLiteralEntities = entSim.matchLiteralEntitiesByWordNet(literalObjects1, literalObjects2)
        var eval: Evaluation = new Evaluation()
        // this works with array similarLiteralEntities
        val compressionRatio2 = eval.compressionRatio(literalObjects1.count() + literalObjects2.count(), similarLiteralEntities.length)
        println("Compression Ration in literal entities merging = " + compressionRatio2 + "%")

      }else{
        val similarLiteralEntities = entSim.matchLiteralEntitiesByWordNetRDD(literalObjects1, literalObjects2)
        val compressionRatio2 = eval.compressionRatio(literalObjects1.count()+literalObjects2.count(), similarLiteralEntities.count())
        println("Compression Ration in literal entities merging = " + compressionRatio2 + "%")
      }


    }

    if (makeResult3) {
      println("//############################ Getting similarity between non-literal (called source in jena definition) entities (Subjects and objects) ####################################")
      // exactly like predicates:

      val objects1 = triplesRDD1.filter(_.getObject.isURI).map(_.getObject.getLocalName).distinct()
      val objects2 = triplesRDD2.filter(_.getObject.isURI).map(_.getObject.getLocalName).distinct()
      val subjects1 = triplesRDD1.map(_.getSubject.getLocalName).distinct()
      val subjects2 = triplesRDD2.map(_.getSubject.getLocalName).distinct()
      val entitiy1 = objects1 ++ subjects1
      val entitiy2 = objects2 ++ subjects2

      println("Entities (with URI) in KG1 are " + entitiy1.count())
      entitiy1.distinct().take(5).foreach(println)
      println("First literal entity " + entitiy1.take(entitiy1.count().toInt).apply(0))

      println("Entities (with URI) in KG2 are " + entitiy2.count()) //81
      entitiy2.distinct().take(5).foreach(println)
      println("First literal entity" + entitiy2.first())

      val subSim = new EntitiesSimilarity(sparkSession.sparkContext)
      var eval: Evaluation = new Evaluation()

      if (!RunWordNetWithRDD) {
        //this creates array:
        val similarEntities = subSim.matchLiteralEntitiesByWordNet(entitiy1, entitiy2)
        // this works with array of similarEntities
        val compressionRatio3 = eval.compressionRatio(entitiy1.count() + entitiy2.count(), similarEntities.length)
        println("Compression Ratio in non-literal Entities merging = " + compressionRatio3 + "%")
      }else{
        val similarEntities = subSim.matchLiteralEntitiesByWordNetRDD(entitiy1,entitiy2)
        val compressionRatio3 = eval.compressionRatio(entitiy1.count()+entitiy2.count(),similarEntities.count())
        println("Compression Ratio in non-literal Entities merging = " + compressionRatio3 + "%")
      }
    }
    sparkSession.stop
  }

}