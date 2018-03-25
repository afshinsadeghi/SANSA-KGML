package net.sansa_stack.kgml.rdf

import java.net.URI

import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Dataset

case class StringTriples(Subject: String, Predicate: String, Object: String)

/**
  * Created by Afshin on 22.02.18.
  */
object ModuleExecutor {

  var input1 = "" // the module name
  var input2 = "" // parameters
  var input3 = ""

  def main(args: Array[String]) = {
    println("running a module...")

    if (args.headOption.isDefined) {
      input1 = args(0)
      if (args.length > 1) {
        input2 = args(1)
      }
      if (args.length > 2) {
        input3 = args(2)
      }

    } else {
      println("module name to run is not set. running with default values:")

      println("current modules are: PredicateStats,CommonPredicateStats,RankByPredicateType,PredicateMatching,PredicatePartitioning," +
        "BlockSubjectsByTypeAndLiteral,CountSameASLinks,EntityMatching")

      input1 = "EntityMatching"
      //input2 = "datasets/dbpediamapping50k.nt"
      //input3 = "datasets/yagofact50k.nt"
      //input2 = "datasets/dbpediaOnlyAppleobjects.nt"
      //input3 = "datasets/yagoonlyAppleobjects.nt"
      input2 = "datasets/dbpediaSimple.nt"
      input3 = "datasets/yagoSimple.nt"
      //input2 = "datasets/drugbank_dump.nt"
      //input3 = "datasets/dbpedia.drugs.nt"
      //input2 = "datasets/person11.nt" //   894 matched
      //input3 = "datasets/person12.nt"
    }
    println(input1)
    println(input2)
    println(input3)
    val gb = 1024 * 1024 * 1024
    val runTime = Runtime.getRuntime
    var memory = (runTime.maxMemory / gb).toInt
    if (memory == 0) memory = 1
    println("dedicated memory:   " + memory + " gb") //1 GB is the default memory of spark

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024")
      .config("spark.sql.broadcastTimeout", "1200")
      .appName("Triple merger of " + input1 + " and " + input2 + " ")
      .getOrCreate()


    //val triplesRDD1 = NTripleReader.load(sparkSession, URI.create(input2)) // RDD[Triple]


    val stringSchema = StructType(Array(
      StructField("Subject", StringType, true),
      StructField("Predicate", StringType, true),
      StructField("Object", StringType, true),
      StructField("dot", StringType, true))
    )


    var DF1 = sparkSession.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", " ")
      .option("comment", "#")
      .option("maxColumns", "4")
      .schema(stringSchema)
      .load(input2)


    var DF2 = sparkSession.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("comment", "#")
      .option("delimiter", " ") // for DBpedia some times it is \t
      .option("maxColumns", "4")
      .schema(stringSchema)
      .load(input3)

    DF1 = DF1.drop(DF1.col("dot"))
    DF2 = DF2.drop(DF2.col("dot"))

    // defining schema and removing duplicates
    val df1 = DF1.toDF("Subject1", "Predicate1", "Object1").dropDuplicates("Subject1", "Predicate1", "Object1")
    val df2 = DF2.toDF("Subject2", "Predicate2", "Object2").dropDuplicates("Subject2", "Predicate2", "Object2")

    if (input1 == "PredicatePartitioning") {
      var partitions = new Partitioning(sparkSession.sparkContext)
      partitions.predicatesDFPartitioningByKey(df2, df2)
    }


    if (input1 == "PredicateStats") {

      val typeStats = new net.sansa_stack.kgml.rdf.TypeStats(sparkSession)
      //typeStats.calculateStats(triplesRDD1, triplesRDD2)
      println("Stats of dataset 1...")
      typeStats.calculateDFStats(df1)
      println("Stats of dataset 2...")
      val df2 = DF2.toDF("Subject1", "Predicate1", "Object1")
      typeStats.calculateDFStats(df2)
    }

    if (input1 == "CommonPredicateStats") {
      val typeStats = new net.sansa_stack.kgml.rdf.TypeStats(sparkSession)
      typeStats.getMaxCommonTypes(df1, df2)
    }

    if (input1 == "RankByPredicateType") {
      val typeStats = new net.sansa_stack.kgml.rdf.TypeStats(sparkSession)
      typeStats.rankDFSubjectsByType(df1, df2)
    }


    if (input1 == "PredicateMatching") {
      val blocking = new net.sansa_stack.kgml.rdf.Blocking(sparkSession)
      profile {
        blocking.getMatchedPredicates(df1, df2)
      }
    }

    if (input1 == "BlockSubjectsByTypeAndLiteral") {

      val SubjectsWithLiteral = profile {
        val matching = new net.sansa_stack.kgml.rdf.Matching(sparkSession)
        val blocking = new net.sansa_stack.kgml.rdf.Blocking(sparkSession)

        val predicatePairs = blocking.getMatchedPredicates(df1, df2)
        blocking.BlockSubjectsByTypeAndLiteral(df1, df2, predicatePairs)
      }
      SubjectsWithLiteral.show(200, 80)
    }
    //idea first round only use string matching on literal objects. Then on next rounds compare parents with parents
    // match them using both wordnet and predicate. if in their childer there are already a child matched, do not match, instead add its smilariy and count that at agregate time.
    // and learn new predicates pairs by that. and compare more .

    //the blocking base on common predicate works for small dataset but not for varient kgs
    //Difference of KG and dataset matching. dedicate a step for that in the paper, and proposal
    // in kg ontology is varient, for example common predicate is good for Person dataset but not debpida
    // there I should rank predicates to find those that are most repeated in the whole dataset. and make itersect of them

    if (input1 == "EntityMatching") {
      val matching = new net.sansa_stack.kgml.rdf.Matching(sparkSession)
      val blocking = new net.sansa_stack.kgml.rdf.Blocking(sparkSession)

      val matchedEntites = profile {

        val dfTripleWithLiteral1 = df1.filter(!col("object1").startsWith("<")).persist()
        val dfTripleWithLiteral2 = df2.filter(!col("object2").startsWith("<")).persist()
        val predicatePairs = blocking.getMatchedPredicates(dfTripleWithLiteral1, dfTripleWithLiteral2)

        val SubjectsWithLiteral = blocking.BlockSubjectsByTypeAndLiteral(dfTripleWithLiteral1, dfTripleWithLiteral2, predicatePairs)
        //first level match using leaf literals
        var subjectsMatch = matching.scheduleLeafMatching(SubjectsWithLiteral, memory).persist()
        //val allPredicatePairs = blocking.getMatchedPredicates(df1, df2)

        //another method could be filtering those who matched by literals, but we grow the matching network
        var parentNodes = blocking.getParentEntities(df1, df2, subjectsMatch)

        while (!parentNodes.take(1).isEmpty) {
          println("In loop to match parents, parents count= " + parentNodes.count())
          val parentSubjectsWithLiteral = blocking.getSubjectsWithLiteral(parentNodes)
          val parentSubjectsMatch = matching.scheduleParentMatching(parentSubjectsWithLiteral, subjectsMatch)
          subjectsMatch = subjectsMatch.union(parentSubjectsMatch)
          parentNodes = blocking.getParentEntities(df1, df2, subjectsMatch)
        }
        subjectsMatch
      }
      matchedEntites.show(20, 80)
      println("number of matched entities pairs: " + matchedEntites.count.toString)
      matchedEntites.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile("../matchedEntities")
    }


    if (input1 == "CountSameASLinks") {
      val typeStats = new net.sansa_stack.kgml.rdf.TypeStats(sparkSession)
      typeStats.countSameASLinks(df1)
    }


    println("end of running Module executor.")

    sparkSession.stop
  }

  def profile[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000000.00 + "s")
    result
  }

}