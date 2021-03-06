package net.sansa_stack.kgml.rdf

import java.net.URI
import java.nio.file.{Files, Paths}

import net.sansa_stack.kgml.rdf.ModuleExecutor.profile
import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

case class StringTriples(Subject: String, Predicate: String, Object: String)

/**
  * Created by Afshin on 22.02.18.
  */
object ModuleExecutor {

  var predicateMatchesPath = "matchedPredicates" // a folder consist of tab separated matched predicates
  var input1 = "" // the module name
  var input2 = "" // parameters
  var input3 = ""
  var input4 = "" //delimiter1
  var input5 = "" //delimiter2
  var input6 = ""
  var delimiter1 = "\t"
  var delimiter2 = "\t"
  var printResults = false
  var wordNetPredicateSimThreshold = 0.7

  def main(args: Array[String]) = {
    println("running a module...")

    //val cores = Runtime.getRuntime.availableProcessors
    //println("number of available cores:" + cores) I used number of cores in repartitioning
    if (args.headOption.isDefined) {
      input1 = args(0) // Module name to run
      if (args.length > 1) {
        input2 = args(1) //First data set path
      }
      if (args.length > 2) {
        input3 = args(2) //Second  data set path
      }
      if (args.length > 3) {
        input4 = args(3) //Column delimiter in the first data set
      }
      if (args.length > 4) {
        input5 = args(4) //Column delimiter in the second data set
      }
      if (args.length > 5) {
        input6 = args(5) //If set to "show" the reporting during execution is shown
      }
    } else {
      println("How to run: --class net.sansa_stack.kgml.rdf.ModuleExecutor input0 input1 input2 input3 input4 input5")
      println("input0: Module name to run")
      println("input1: First data set path")
      println("input2: Second  data set path")
      println("input3: Column delimiter in the first data set")
      println("input4: Column delimiter in the second data set")
      println("input5: If set to \\\"show\\\" the reporting during execution is shown")
      println("module name to run is not set. running with default values:")
      println("current modules are: PredicateStats,CommonPredicateStats,RankByPredicateType,PredicateMatching,PredicatePartitioning," +
        "ClusterSubjectsByType,CountSameASLinks,EntityMatching", "deductRelations", "WordNetEvaluation1", "WordNetEvaluation2", "PredicateExactMatch")

      println("Sample run1:  EntityMatching datasets/person11.nt datasets/person12.nt space space show")
      println("Sample run2:  PredicateExactMatch datasets/dbpediaMovies.nt datasets/yagoMovies.nt tab tab hide")

      // setting input1 to deductRelations when doing entity matching, it performs an extra step of relation extraction using current relations.
      input1 = "EntityMatching" // "EntityMatching" and etc (the list above)

      //input2 = "datasets/dbpediamapping50k.nt"
      //input3 = "datasets/yagofact50k.nt"
      //input2 = "datasets/dbpediaOnlyAppleobjects.nt"
      //input3 = "datasets/yagoonlyAppleobjects.nt"
      //input2 = "datasets/dbpediaSimple.nt"
      //input3 = "datasets/yagoSimple.nt"
      //input2 = "datasets/drugbank_dump.nt"
      //input3 = "datasets/dbpedia.drugs.nt"
      input2 = "datasets/person11.nt" //   894 matched
      input3 = "datasets/person12.nt"
      //input2 = "datasets/abstract1.nt" // To test extracting new relations on iterations:
      //input3 = "datasets/abstract2.nt" // Correctness condition: person2 and person4 are not matched by literals but their equivalency should be discovered by deduction.
      //input2 = "datasets/commonPredicatesTest.nt" // To test that only matched subjects are in same blocks:
      //input3 = "datasets/commonPredicates2Test.nt" // Correctness condition:person1 and car1 are not in any block of 2 common predicates
      //input2 = "datasets/dbpediaMovies.nt"
      //input3 = "datasets/linkedmdb-2010.nt"
      //input3 = "datasets/yagoMovies.nt"
      input4 = "space" // delimiter can be or space.For dbpediaMovies and yagoMovies its tab and for person is space
      input5 = "space" // delimiter default is tab. it can be space
      input6 = ""
    }
    println(input1)
    println("First data set: " + input2)
    println("Second data set: " + input3)
    println("delimiter 1: " + input4)
    println("delimiter 2: " + input5)
    if (input4 == "tab") this.delimiter1 = "\t" else this.delimiter1 = " "
    if (input5 == "tab") this.delimiter2 = "\t" else this.delimiter2 = " "
    if (input6 == "show") this.printResults = true

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
      //.config("spark.driver.maxResultSize", "3g") set in input command line instead: --conf spark.driver.maxResultSize=6g
      .appName("Entity matching for " + input2 + " and " + input3 + " ")
      .getOrCreate()

    //val triplesRDD1 = NTripleReader.load(sparkSession, URI.create(input2)) // RDD[Triple]

    val dataReader = new ReadDataSet
    var df1 = dataReader.load(sparkSession, input2, delimiter1).toDF("Subject1", "Predicate1", "Object1")
    var df2 = dataReader.load(sparkSession, input3, delimiter2).toDF("Subject2", "Predicate2", "Object2")

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
      df2 = df2.toDF("Subject1", "Predicate1", "Object1")
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
      if (Files.exists(Paths.get(predicateMatchesPath))) {
        println("predicate match folder already exists:" + predicateMatchesPath)
        val predicatePairs = sparkSession.read.format("com.databricks.spark.csv")
          .option("header", "false")
          .option("inferSchema", "false")
          .option("delimiter", "\t")
          .option("comment", "#")
          .option("maxColumns", "4")
          .load(predicateMatchesPath).toDF("predicate1", "predicate2")
        predicatePairs.show(30, 50)
      } else {

        val predicatePairs = profile {
          val simHandler = new SimilarityHandler(simThreshold)
          val blocking = new net.sansa_stack.kgml.rdf.Blocking(sparkSession, simHandler)
          blocking.printReport = printResults
          blocking.getMatchedPredicates(df1, df2, wordNetPredicateSimThreshold)
        }
        predicatePairs.write.format("com.databricks.spark.csv").option("header", "false")
          .option("inferSchema", "false")
          .option("delimiter", "\t").save(predicateMatchesPath)
      }
    }

    if (input1 == "ClusterSubjectsByType") {

      val simHandler = new SimilarityHandler(simThreshold)
      val matching = new net.sansa_stack.kgml.rdf.Matching(sparkSession, simHandler)
      val blocking = new net.sansa_stack.kgml.rdf.Blocking(sparkSession, simHandler)

      blocking.printReport = this.printResults
      matching.printReport = this.printResults
      if (!Files.exists(Paths.get(predicateMatchesPath))) {
        println("Predicate pairs does not exist, run the module PredicateMatching first to create the match table,then try again.")
        System.exit(1)
      }
      println("Using predicate match folder : " + predicateMatchesPath)
      val predicatePairs = sparkSession.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("delimiter", "\t")
        .option("comment", "#")
        .option("maxColumns", "4")
        .load(predicateMatchesPath).toDF("predicate1", "predicate2")

      val dfLiteral1 = df1.filter(!col("object1").startsWith("<"))//.persist()
      val dfLiteral2 = df2.filter(!col("object2").startsWith("<"))//.persist()
      df1.unpersist()
      df2.unpersist()
      val clusteredSubjects = profile {
        val SubjectsWithLiteral = blocking.blockSubjectsByTypeAndLiteral(dfLiteral1, dfLiteral2, predicatePairs)
        dfLiteral1.unpersist()
        dfLiteral2.unpersist()
        predicatePairs.unpersist() // to free disk space
        val clusteredSubjects = matching.clusterSubjects(SubjectsWithLiteral) //clusteredSubjects with number of predicates that matched
        clusteredSubjects.show(50, 80) // adding show here to count real time usage
        clusteredSubjects
      }

    }
    //idea first round only use string matching on literal objects. Then on next rounds compare parents with parents
    // match them using both wordnet and predicate. if in their childer there are already a child matched, do not match, instead add its smilariy and count that at agregate time.
    // and learn new predicates pairs by that. and compare more .

    //the blocking base on common predicate works for small dataset but not for varient kgs
    //Difference of KG and dataset matching. dedicate a step for that in the paper, and proposal
    // in kg ontology is varient, for example common predicate is good for Person dataset but not debpida
    // there I should rank predicates to find those that are most repeated in the whole dataset. and make itersect of them

    if (input1 == "EntityMatching" || input1 == "deductRelations") {
      val simHandler = new SimilarityHandler(simThreshold)
      val matching = new net.sansa_stack.kgml.rdf.Matching(sparkSession, simHandler)
      val blocking = new net.sansa_stack.kgml.rdf.Blocking(sparkSession, simHandler)
      val rExtractor = new net.sansa_stack.kgml.rdf.RelationExtractor

      matching.printReport = this.printResults
      blocking.printReport = this.printResults
      val matchedEntities = profile {

        //   val dfTripleWithLiteral1 = df1.filter(!col("object1").startsWith("<"))

        // val dfTripleWithLiteral2 = df2.filter(!col("object2").startsWith("<")) //.persist()

        // filter literals for blocking  , leaf blocking is only uses literals, deduction level uses the whole df1 and df2
        val dfLiteral1 = df1.filter(!col("object1").startsWith("<"))//.persist()
        val dfLiteral2 = df2.filter(!col("object2").startsWith("<"))//.persist()
        //val dfNoLiteral1 = df1.filter(col("object1").startsWith("<"))//.persist()
        //val dfNoLiteral2 = df2.filter(col("object2").startsWith("<"))//.persist()

        import sparkSession.implicits._
        var predicatePairs = Seq.empty[(String, String)].toDF("predicate1", "predicate2") // matched Predicate Pairs, to initialize
        if (Files.exists(Paths.get(predicateMatchesPath))) {
          predicatePairs = sparkSession.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("inferSchema", "false")
            .option("delimiter", "\t")
            .option("comment", "#")
            .option("maxColumns", "4")
            .load(predicateMatchesPath).toDF("predicate1", "predicate2")
        } else {
          predicatePairs = blocking.getMatchedPredicates(df1, df2, wordNetPredicateSimThreshold)
          predicatePairs.write.format("com.databricks.spark.csv").option("header", "false")
            .option("inferSchema", "false")
            .option("delimiter", "\t").save(predicateMatchesPath)
        }

        val SubjectsWithLiteral = blocking.blockSubjectsByTypeAndLiteral(dfLiteral1, dfLiteral2, predicatePairs)
        //first level match using leaf literals
        val clusteredSubjects = matching.clusterSubjects(SubjectsWithLiteral)
        var subjectsMatch = matching.scheduleLeafMatching(SubjectsWithLiteral, clusteredSubjects, memory).persist()
        //val allPredicatePairs = blocking.getMatchedPredicates(df1, df2)

        if (input1 == "deductRelations") {
          //another method could be filtering those who matched by literals, but we grow the matching network
          var parentNodes1 = rExtractor.getParentEntities(df1, subjectsMatch.select("subject1").toDF("Subject3"))
          var parentNodes2 = rExtractor.getParentEntities(df2.toDF("Subject1", "Predicate1", "Object1"),
            subjectsMatch.select("subject2").toDF("Subject3")).toDF("Subject2", "Predicate2", "Object2")

          parentNodes1 = rExtractor.replaceMatched(parentNodes1, subjectsMatch.select("subject1", "subject2")
            .toDF("Subject3", "Subject4"))
          predicatePairs = blocking.getMatchedPredicates(parentNodes1, parentNodes2, wordNetPredicateSimThreshold)
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
            predicatePairs = blocking.getMatchedPredicates(parentNodes1, parentNodes2, wordNetPredicateSimThreshold)
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

    //evaluation of WordNet with exact comparison in matching, here we do not filter objects for literals
    if (input1 == "WordNetEvaluation1" || input1 == "WordNetEvaluation2") {
      val simHandler = new SimilarityHandler(simThreshold)
      val matching = new net.sansa_stack.kgml.rdf.Matching(sparkSession, simHandler)

      matching.printReport = this.printResults

      if (input1 == "WordNetEvaluation1") {
        matching.exactMatchEvaluation = true
        matching.wordNetMatchEvaluation = false

      } else {
        matching.exactMatchEvaluation = false
        matching.wordNetMatchEvaluation = true
      }

      val blocking = new net.sansa_stack.kgml.rdf.Blocking(sparkSession, simHandler)
      blocking.printReport = this.printResults

      val matchedEntities = profile {
        import sparkSession.implicits._
        var predicatePairs = Seq.empty[(String, String)].toDF("predicate1", "predicate2") // matched Predicate Pairs, to initialize
        if (Files.exists(Paths.get(predicateMatchesPath))) {
          predicatePairs = sparkSession.read.format("com.databricks.spark.csv")
            .option("header", "false")
            .option("inferSchema", "false")
            .option("delimiter", "\t")
            .option("comment", "#")
            .option("maxColumns", "4")
            .load(predicateMatchesPath).toDF("predicate1", "predicate2")
        } else {
          predicatePairs = blocking.getMatchedPredicates(df1, df2, wordNetPredicateSimThreshold)
          predicatePairs.write.format("com.databricks.spark.csv").option("header", "false")
            .option("inferSchema", "false")
            .option("delimiter", "\t").save(predicateMatchesPath)
        }
        val SubjectsWithLiteral = blocking.blockSubjectsByTypeAndLiteral(df1, df2, predicatePairs)
        df1.unpersist()
        df2.unpersist()
        predicatePairs.unpersist() //to free disk space
        val clusteredSubjects = matching.clusterSubjects(SubjectsWithLiteral)
        //val subjectsMatch = matching.scheduleLeafMatching(SubjectsWithLiteral, clusteredSubjects, memory)
        clusteredSubjects.show(10,30)  // adding show here to count real time usage in evaluation
        clusteredSubjects
      }
        println("number of matched entities " + matchedEntities.count())

      matchedEntities.write.format("com.databricks.spark.csv").option("header", "false")
        .option("inferSchema", "false")
        .option("delimiter", "\t").save(input1)
    }



    // to compare WordNet predicate match with exact match, and for evaluation of blocking with different version of same
    // data set here we do exact match
    if (input1 == "PredicateExactMatch") {
      if (Files.exists(Paths.get(predicateMatchesPath))) {
        println("predicate match folder already exists:" + predicateMatchesPath)
        val predicatePairs = sparkSession.read.format("com.databricks.spark.csv")
          .option("header", "false")
          .option("inferSchema", "false")
          .option("delimiter", "\t")
          .option("comment", "#")
          .option("maxColumns", "4")
          .load(predicateMatchesPath).toDF("predicate1", "predicate2")
        predicatePairs.show(30, 50)
      } else {

        val predicatePairs = profile {
          val simHandler = new SimilarityHandler(simThreshold)
          val blocking = new net.sansa_stack.kgml.rdf.Blocking(sparkSession, simHandler)
          blocking.printReport = printResults
          blocking.getEqualPredicates(df1, df2)
        }
        predicatePairs.write.format("com.databricks.spark.csv").option("header", "false")
          .option("inferSchema", "false")
          .option("delimiter", "\t").save(predicateMatchesPath)
      }
    }


    //different data sets have different number of same as link, they influence the linking problem
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