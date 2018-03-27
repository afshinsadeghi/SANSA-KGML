package net.sansa_stack.kgml.rdf


import java.net.URI

import net.sansa_stack.kgml.rdf.ModuleExecutor.{input2, input3}
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


object TestMatchingResult {
  def main(args: Array[String]): Unit = {
    input2 = "datasets/results/goldstandard_person_1.nt"
    input3 = "datasets/results/person1_matched.nt" //   894 matched

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024")
      .config("spark.sql.broadcastTimeout", "1200")
      .appName("Testing entity matching ")
      .getOrCreate()


    val stringSchema = StructType(Array(
      StructField("Subject", StringType, true),
      StructField("Predicate", StringType, true),
      StructField("Object", StringType, true)
    ))


    var DF1 = sparkSession.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", " ")
      .option("maxColumns", "3")
      .schema(stringSchema)
      .load(input2)


    var DF2 = sparkSession.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("delimiter", " ") // for DBpedia some times it is \t
      .option("maxColumns", "3")
      .schema(stringSchema)
      .load(input3)


    // defining schema and removing duplicates
    val df1 = DF1.toDF("Subject1", "Subject2", "score").drop("score").dropDuplicates("Subject1", "Subject2").persist()
    val df21 = DF2.toDF("Subject1", "Subject2", "score").drop("score").dropDuplicates("Subject1", "Subject2").persist()
    val df22 = DF2.toDF("Subject2", "Subject1", "score").drop("score").dropDuplicates("Subject1", "Subject2").persist()

    val df2 = df21.union(df22)

    println(df1.count())
    println(df2.count()/2)

    var df3 = df1.except(df2)
    println("number of matches of gold standard not in results: " + df3.count())

    var df4 = df2.except(df1)
    df4.show(40, 50)
    println("Number those non addresses(persons) that are matched in my method:")
    println(df2.except(df2.where(col("Subject1").contains("Address") && col("Subject2").contains("Address"))).count())

    println("Number those  addresses that are matched in my method:")
    println(df4.where(col("Subject1").contains("Address") && col("Subject2").contains("Address")).count())

    println("precision and recall are 1")
  }

}
