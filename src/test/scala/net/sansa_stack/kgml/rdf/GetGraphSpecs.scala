/**
  * Created by afshin on 10.10.17.
  */

package net.sansa_stack.kgml.rdf
import java.net.URI
import scala.collection.mutable
import org.apache.spark.sql.SparkSession
import net.sansa_stack.rdf.spark.io.NTripleReader
import java.io._

//import ml.dmlc.mxnet._
//import ml.dmlc.mxnet.optimizer.SGD

/*
    This object is get print information of graphs for to analyse the merging work
 */
object GetGraphsSpecs {


  def main(args: Array[String]) = {
    // Testing if MXBNet works
    //val aux = NDArray.zeros(11886, 916, 11285)
    // println("made a big matrix!")

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


    //triplesRDD.take(5).foreach(println(_))
    triplesRDD.cache()

    println("Number of triples in the first KB\n")   //dbpedia 2457
    println(triplesRDD.count().toString)

    println("Number of triples in the second KB\n") //yago 738
    println(triplesRDD2.count().toString)


    val subject = triplesRDD.map(f => (f.getSubject, 1))

    val subjectCount = subject.reduceByKey(_ + _)
    println("SubjectCount \n")
    subjectCount.take(5).foreach(println(_))

    // predicates storage in  seperated files
    val predicates = triplesRDD.map(f => (f.getPredicate, 1))
    val predicateCount = predicates.reduceByKey(_ + _)
    val writer1 = new PrintWriter(new File("src/main/resources/DBpedia_Predicates.nt" ))
    predicateCount.collect().sortBy(f=> f._2  * -1 ).foreach(x=>{writer1.write(x.toString()+ "\n")})
    writer1.close()


    val predicates2 = triplesRDD2.map(f => (f.getPredicate, 1))
    val predicateCount2 = predicates2.reduceByKey(_ + _)
    val writer2 = new PrintWriter(new File("src/main/resources/YAGO_Predicates.nt" ))
    predicateCount2.collect().sortBy(f=> f._2  * -1 ).foreach(x=>{writer2.write(x.toString()+ "\n")})
    writer2.close()



    //a preview of predicates
    println("PredicateCount of first RDD \n" + predicateCount.count())
    predicateCount.take(20).sortBy(f=> f._2  * -1 ).foreach(println(_))
    var SumAllPredicates =  predicates.distinct().count()
    println("20 Predicates from first RDD " + SumAllPredicates + "\n")
    println(SumAllPredicates)  // 333



    println("PredicateCount of second RDD \n" + predicateCount2.count())
    predicateCount2.take(20).sortBy(f=> f._2  * -1 ).foreach(println(_))
    var SumAllPredicates2 =  predicates2.distinct().count()
    println(" 20 Predicates from second RDD " + SumAllPredicates2 + "\n")
    println(SumAllPredicates2) //83

    // above calculations in one line
    //println(" 10 from Predicates in first RDD ")
    // triplesRDD2.map(f => (f.getPredicate, 1)).take(10).foreach(println(_))


    sparkSession.stop
    sparkSession2.stop

  }

}