package net.sansa_stack.kgml.rdf

import java.nio.file.{Files, Paths}

import net.sansa_stack.kgml.rdf.ModuleExecutor.{delimiter1, delimiter2, input2, input3}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.dmg.pmml.Delimiter


class ReadDataSet extends EvaluationHelper {

  def load(sparkSession: SparkSession, inputFile: String, delimiter1: String): DataFrame = {
    val stringSchema = StructType(Array(
      StructField("Subject", StringType, true),
      StructField("Predicate", StringType, true),
      StructField("Object", StringType, true),
      StructField("dot", StringType, true))
    )

    if (!Files.exists(Paths.get(inputFile))) {
      println("input nt data set does not exist:" + inputFile)
      System.exit(1)
    }

    var DF1 = sparkSession.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .option("comment", "#")
      .option("delimiter", delimiter1) // for DBpedia some times it is \t
      .option("maxColumns", "4")
      .schema(stringSchema)
      .load(inputFile)

    DF1 = DF1.drop(DF1.col("dot"))

    // defining schema and removing duplicates
    val df = preProcess(DF1)
    if (printReport) {
      println("Input data set:")
      df.show(8, 60)
    }
    df
  }

  def preProcess(DF: DataFrame): DataFrame = {
    //remove duplicates
    var df = DF.toDF("Subject1", "Predicate1", "Object1").dropDuplicates("Subject1", "Predicate1", "Object1") //.persist()
    // remove meta relations, these predicates mislead the blocking, because two cluster of different subjects have this predicate
    df = df.where(df("Predicate1") =!= "<http://www.w3.org/2002/07/owl#sameAs>")
    df = df.where(df("Predicate1") =!= "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")
    df
  }

}
