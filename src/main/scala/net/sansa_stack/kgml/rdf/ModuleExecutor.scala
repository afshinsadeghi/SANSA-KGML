package net.sansa_stack.kgml.rdf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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

      input1 = "TypeStats"
      //input2 = "datasets/dbpediamapping5k.nt"
      //input3 = "datasets/yagofact5k.nt"
      input2 = "datasets/dbpediaOnlyAppleobjects.nt"
      input3 = "datasets/yagoonlyAppleobjects.nt"
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
      StructField("Object", StringType, true)))

    val DF1 = sparkSession.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", false)
      .option("delimiter", "\t")
      .schema(stringSchema)
      .load(input2)


    val DF2 = sparkSession.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", false)
      .option("delimiter", "\t")
      .schema(stringSchema)
      .load(input3)

    val df1= DF1.toDF("Subject","Predicate","Object")
    val df2= DF2.toDF("Subject","Predicate","Object")

//    df1.take(10).foreach(println)
//    val predicatesWithKeys1 = df1.map(_.getPredicate.getLocalName).distinct().zipWithIndex()
    val predicateDF1 = df1.select("Predicate").distinct()
    val predicateDF2 = df2.select("Predicate").distinct()

    var partitions = new Partitioning()
    partitions.predicatesDFPartitioningByKey(predicateDF1, predicateDF2)




   // val triplesRDD2 = NTripleReader.load(sparkSession, URI.create(input3))

    // val df2: DataFrame = triplesRDD2.toDF this does not work so we read it as dataframe from begining

    if (input1 == "TypeStats") {

      val typeStats = new net.sansa_stack.kgml.rdf.TypeStats(sparkSession)
      //typeStats.calculateStats(triplesRDD1, triplesRDD2)
      println("Stats of dataset 1...")
      typeStats.calculateDFStats(df1)
      println("Stats of dataset 2...")
      typeStats.calculateDFStats(df2)
    }

    println("end of running Module executor.")

    sparkSession.stop
  }

}