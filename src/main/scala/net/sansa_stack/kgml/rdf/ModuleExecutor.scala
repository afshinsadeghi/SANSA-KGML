package net.sansa_stack.kgml.rdf

import java.net.URI

import net.sansa_stack.rdf.spark.io.NTripleReader
import org.apache.spark.sql.SparkSession


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
      println("please specify module name to run. running with default values")

      input1 = "TypeStats"
      input2 = "datasets/dbpediamapping5k.nt"
      input3 = "datasets/yagofact5k.nt"
    }

    val sparkSession = SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "1024")
      .config("spark.kryo.registrator", "net.sansa_stack.kgml.rdf.Registrator")
      .appName("Triple merger of " + input1 + " and " + input2 + " ")
      .getOrCreate()

    val triplesRDD1 = NTripleReader.load(sparkSession, URI.create(input2)) // RDD[Triple]
    val triplesRDD2 = NTripleReader.load(sparkSession, URI.create(input3))

    if (input1 == "TypeStats"){

      val typeStats = new net.sansa_stack.kgml.rdf.TypeStats()
      typeStats.calculateStats(triplesRDD1, triplesRDD2)

      println("end of running Module executor.")
    }


    sparkSession.stop
  }

}