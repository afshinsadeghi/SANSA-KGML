package net.sansa_stack.kgml.rdf

import java.net.URI

import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

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

      println("current modules are: TypeStats,CommonTypes,RankByType,PredicateMatching,PredicatePartitioning," +
        "BlockSubjectsByTypeAndLiteral,CountSameASLinks")

      input1 = "BlockSubjectsByTypeAndLiteral"
      //input2 = "datasets/dbpediamapping50k.nt"
      //input3 = "datasets/yagofact50k.nt"
      input2 = "datasets/dbpediaOnlyAppleobjects.nt"
      input3 = "datasets/yagoonlyAppleobjects.nt"
      //input2 = "datasets/DBpediaAppleSmalldataset.nt"
      //input3 = "datasets/YagoAppleSmallDataset.nt"
      //input2 = "datasets/drugbank_dump.nt"
      //input3 = "datasets/dbpedia.drugs.nt"
      //input2 = "datasets/person21.nt"
      //input3 = "datasets/person22.nt"

    }
    println(input1)
    println(input2)
    println(input3)

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024")
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
      .option("delimiter", "\t")
      .option("maxColumns", "4")
      .schema(stringSchema)
      .load(input2)


    var DF2 = sparkSession.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", "\t")  // for DBpedia it is \t
      .option("maxColumns", "4")
      .schema(stringSchema)
      .load(input3)

   DF1 = DF1.drop(DF1.col("dot"))
   DF2 = DF2.drop(DF2.col("dot"))

    val df1 = DF1.toDF("Subject1", "Predicate1", "Object1")
    val df2 = DF2.toDF("Subject2", "Predicate2", "Object2")

    //df1.drop(df1.col("dot"))
    //df2.drop(df2.col("dot"))

    //removing duplicates
    df1.createTempView("x")
    sparkSession.sql("SELECT subject1, predicate1, object1  FROM( SELECT *, ROW_NUMBER()OVER(PARTITION BY subject1 ORDER BY subject1 DESC) rn FROM x) predicate1 WHERE rn = 1").collect

    //removing duplicates
    df2.createOrReplaceTempView("x")
    sparkSession.sql("SELECT subject2, predicate2, object2  FROM( SELECT *, ROW_NUMBER()OVER(PARTITION BY subject2 ORDER BY subject2 DESC) rn FROM x) predicate2 WHERE rn = 1").collect




    // val triplesRDD2 = NTripleReader.load(sparkSession, URI.create(input3))
    // val df2: DataFrame = triplesRDD2.toDF this does not work so we read it as dataframe from begining


    if (input1 == "PredicatePartitioning") {
      var partitions = new Partitioning(sparkSession.sparkContext)
      partitions.predicatesDFPartitioningByKey(df2, df2)
    }


    if (input1 == "TypeStats") {

      val typeStats = new net.sansa_stack.kgml.rdf.TypeStats(sparkSession)
      //typeStats.calculateStats(triplesRDD1, triplesRDD2)
      println("Stats of dataset 1...")
      typeStats.calculateDFStats(df1)
      println("Stats of dataset 2...")
      val df2 = DF2.toDF("Subject1", "Predicate1", "Object1")
      typeStats.calculateDFStats(df2)
    }

    if (input1 == "CommonTypes") {
      val typeStats = new net.sansa_stack.kgml.rdf.TypeStats(sparkSession)
      typeStats.getMaxCommonTypes(df1, df2)
    }

    if (input1 == "RankByType") {
      val typeStats = new net.sansa_stack.kgml.rdf.TypeStats(sparkSession)
      typeStats.RankDFSubjectsByType(df1, df2)
    }


    if (input1 == "PredicateMatching") {
      val matching = new net.sansa_stack.kgml.rdf.Matching(sparkSession)
      profile {
        matching.getMatchedPredicates(df1, df2)
      }
    }

    if (input1 == "BlockSubjectsByTypeAndLiteral") {
      val matching = new net.sansa_stack.kgml.rdf.Matching(sparkSession)
      val predicatePairs = profile {
        matching.getMatchedPredicates(df1, df2)
      }
      val SubjectsWithLiteral =  profile {
       matching.BlockSubjectsByTypeAndLiteral(df1, df2, predicatePairs)
      }
      val matchedEntites = profile {
         matching.getMatchedEntities(SubjectsWithLiteral, predicatePairs)
      }

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