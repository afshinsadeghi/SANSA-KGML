package net.sansa_stack.kgml.rdf

import java.net.URI
import java.nio.file.{Files, Paths}

import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class StringTriples(Subject: String, Predicate: String, Object: String)

/**
  * Created by Afshin on 22.02.18.
  */
object ModuleExecutor {

  var predicateMatchesPath = "predicatesMatch.csv"
  var predicatePairs: DataFrame // matched Predicate Pairs
  var input1 = "" // the module name
  var input2 = "" // parameters
  var input3 = ""
  var input4 = "deductRelations" // setting input4 to deductRelations when doing entity matching, it performs an extra step of relation extraction using current relations.

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
      if (args.length > 3) {
        input4 = args(3)
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
      //input2 = "datasets/dbpediaSimple.nt"
      //input3 = "datasets/yagoSimple.nt"
      //input2 = "datasets/drugbank_dump.nt"
      //input3 = "datasets/dbpedia.drugs.nt"
      //input2 = "datasets/person11.nt" //   894 matched
      //input3 = "datasets/person12.nt"
      //input2 = "datasets/abstract1.nt" // To test extracting new relations on iterations:
      //input3 = "datasets/abstract2.nt" // Correctness condition: person2 and person4 are not matched by literals but their equivalency should be discovered by deduction.
      //input2 = "datasets/commonPredicatesTest.nt" // To test that only matched subjects are in same blocks:
      //input3 = "datasets/commonPredicates2Test.nt" // Correctness condition:person1 and car1 are not in any block of 2 common predicates
      input2 = "datasets/dbpediaMovies.nt"
      //input3 = "datasets/linkedmdb-2010.nt"
      input3 = "datasets/yagoMovies.nt"
      input4 = "none"

    }
    println(input1)
    println(input2)
    println(input3)
    println(input4)
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
      .appName("Entity matching for " + input2 + " and " + input3 + " ")
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
      .option("delimiter", "\t")
      .option("comment", "#")
      .option("maxColumns", "4")
      .schema(stringSchema)
      .load(input2)


    var DF2 = sparkSession.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("comment", "#")
      .option("delimiter", "\t") // for DBpedia some times it is \t
      .option("maxColumns", "4")
      .schema(stringSchema)
      .load(input3)

    DF1 = DF1.drop(DF1.col("dot"))
    DF2 = DF2.drop(DF2.col("dot"))

    // defining schema and removing duplicates
    val df1 = DF1.toDF("Subject1", "Predicate1", "Object1").dropDuplicates("Subject1", "Predicate1", "Object1").persist()
    val df2 = DF2.toDF("Subject2", "Predicate2", "Object2").dropDuplicates("Subject2", "Predicate2", "Object2").persist()

    val simThreshold = 0.7
    if (input1 == "PredicatePartitioning") {
      var partitions = new Partitioning(sparkSession.sparkContext)
      partitions.predicatesDFPartitioningByKey(df2, df2)
    }


    if (input1 == "PredicateStats") {

      val typeStats = new net.sansa_stack.kgml.rdf.TypeStats(sparkSession)
      //typeStats.calculateStats(triplesRDD1, triplesRDD2)
      println("Stats of data set 1...")
      typeStats.calculateDFStats(df1)
      println("Stats of data set 2...")
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

      val simHandler = new SimilarityHandler(simThreshold)
      val blocking = new net.sansa_stack.kgml.rdf.Blocking(sparkSession, simHandler)

      val predicatePairs = profile {
        blocking.getMatchedPredicates(df1, df2)
      }
      predicatePairs.write.format("com.databricks.spark.csv").save(predicateMatchesPath)

    }

    if (input1 == "BlockSubjectsByTypeAndLiteral") {

      val simHandler = new SimilarityHandler(simThreshold)

      val SubjectsWithLiteral = profile {
        val matching = new net.sansa_stack.kgml.rdf.Matching(sparkSession, simHandler)
        val blocking = new net.sansa_stack.kgml.rdf.Blocking(sparkSession, simHandler)

        val predicatePairs = blocking.getMatchedPredicates(df1, df2)
        blocking.blockSubjectsByTypeAndLiteral(df1, df2, predicatePairs)
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
      val simHandler = new SimilarityHandler(simThreshold)
      val matching = new net.sansa_stack.kgml.rdf.Matching(sparkSession, simHandler)
      val blocking = new net.sansa_stack.kgml.rdf.Blocking(sparkSession, simHandler)
      val rExtractor = new net.sansa_stack.kgml.rdf.RelationExtractor

      val matchedEntities = profile {

        //   val dfTripleWithLiteral1 = df1.filter(!col("object1").startsWith("<"))

        // val dfTripleWithLiteral2 = df2.filter(!col("object2").startsWith("<")) //.persist()

        //val dfNoLiteral1 = df1.filter(col("object1").startsWith("<"))//.persist()
        //val dfNoLiteral2 = df2.filter(col("object2").startsWith("<"))//.persist()

        if (Files.exists(Paths.get(predicateMatchesPath))) {
          predicatePairs = sparkSession.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("inferSchema", "false")
            .option("delimiter", "\t")
            .option("comment", "#")
            .option("maxColumns", "4")
            .schema(stringSchema)
            .load(predicateMatchesPath)
        } else {
          predicatePairs = blocking.getMatchedPredicates(df1, df2)
          predicatePairs.write.format("com.databricks.spark.csv").save(predicateMatchesPath)
        }


        val SubjectsWithLiteral = blocking.blockSubjectsByTypeAndLiteral(df1, df2, predicatePairs)
        //first level match using leaf literals
        var subjectsMatch = matching.scheduleLeafMatching(SubjectsWithLiteral, memory).persist()
        //val allPredicatePairs = blocking.getMatchedPredicates(df1, df2)

        if (input4 == "deductRelations") {
          //another method could be filtering those who matched by literals, but we grow the matching network
          var parentNodes1 = rExtractor.getParentEntities(df1, subjectsMatch.select("subject1").toDF("Subject3"))
          var parentNodes2 = rExtractor.getParentEntities(df2.toDF("Subject1", "Predicate1", "Object1"),
            subjectsMatch.select("subject2").toDF("Subject3")).toDF("Subject2", "Predicate2", "Object2")

          parentNodes1 = rExtractor.replaceMatched(parentNodes1, subjectsMatch.select("subject1", "subject2")
            .toDF("Subject3", "Subject4"))
          predicatePairs = blocking.getMatchedPredicates(parentNodes1, parentNodes2)
          var parentTriples = blocking.blockSubjectsByTypeAndLiteral(parentNodes1, parentNodes2, predicatePairs)

          var counter = 0
          //  subjectsMatch.rdd.map(_.toString().replace("[", "").replace("]", "")).saveAsTextFile(input2 + counter.toString)
          while (counter < 1) {
            counter = counter + 1
            println("In loop to match parents, parents count= " + parentTriples.count())
            val parentSubjectsMatch = matching.scheduleParentMatching(parentTriples) //, subjectsMatch)
            subjectsMatch = subjectsMatch.union(parentSubjectsMatch)
            parentNodes1 = rExtractor.getParentEntities(df1, subjectsMatch.select("subject1").toDF("Subject3"))
            parentNodes2 = rExtractor.getParentEntities(df2.toDF("Subject1", "Predicate1", "Object1"),
              subjectsMatch.select("subject2").toDF("Subject3")).toDF("Subject2", "Predicate2", "Object2")

            parentNodes1 = rExtractor.replaceMatched(parentNodes1, subjectsMatch.select("subject1", "subject2")
              .toDF("Subject3", "Subject4"))
            predicatePairs = blocking.getMatchedPredicates(parentNodes1, parentNodes2)
            parentTriples = blocking.blockSubjectsByTypeAndLiteral(parentNodes1, parentNodes2, predicatePairs)
            subjectsMatch.rdd.map(_.toString().replace("[", "").replace("]", "")).saveAsTextFile(input2 + counter.toString)

          }
        }
        subjectsMatch
      }

      //     matchedEntities.rdd.map(_.toString().replace("[","").replace("]", "")).saveAsTextFile("../matchedEntities")
      matchedEntities.show(20, 80)
      println("number of matched entities pairs: " + matchedEntities.count.toString)
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