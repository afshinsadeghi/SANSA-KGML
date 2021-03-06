package net.sansa_stack.kgml.rdf

import java.util.Objects

import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
/*
* Created by Shimaa
*
* */

class Partitioning (sc : SparkContext){
  /*
  * This class takes two rdd for prdicates (one rdd for each dataset) and partitioned each rdd separtely, then apply cartesian product on the partitioned rdd
  * */
  def predicatesRDDPartitioningByKey(Predicates1 : RDD[(String, Long)], Predicates2 : RDD[(String, Long)]) = {


    println("Length of predicate 1: "+Predicates1.count()) //60
    println("Length of predicate 2: "+Predicates2.count()) //28

//    //Partitioning using coalesce takes 8 ms
    val predicate_1_partitioned = Predicates1.coalesce(4).persist()
    val predicate_2_partitioned = Predicates2.coalesce(4).persist()

//    //Partitioning using repartition takes 11 ms
//    val predicate_1_partitioned = Predicates1.repartition(4).persist()
//    val predicate_2_partitioned = Predicates2.repartition(4).persist()

    //Range partition takes 17 ms
//        val predicate_1_partitioned = Predicates1.partitionBy(new RangePartitioner(4, Predicates1)).persist()
//        val predicate_2_partitioned = Predicates2.partitionBy(new RangePartitioner(4, Predicates2)).persist()

    //Hash partition takes 10 ms
//          val predicate_1_partitioned = Predicates1.partitionBy(new HashPartitioner(4)).persist()
//          val predicate_2_partitioned = Predicates2.partitionBy(new HashPartitioner(4)).persist()

    predicate_1_partitioned.foreachPartition(iterator => println("elements in this partition for Predicate 1: "+ iterator.length))
    predicate_2_partitioned.foreachPartition(iterator => println("elements in this partition for Predicate 2:"+ iterator.length))

    //val tunedPartitioner = new RangePartitioner(8, Predicates1)
    //println("tunedPartitioner "+ tunedPartitioner + tunedPartitioner.toString+ tunedPartitioner.numPartitions)
    //val predicate_1_partitioned = Predicates1.partitionBy(tunedPartitioner).persist()
    //val tunedPartitioner = new DomainNamePartitioner(4, Predicates1)
    //val predicate_1_partitioned = Predicates1.partitionBy(new DomainNamePartitioner(4, Predicates1)).persist()


    //    Predicates1.repartitionAndSortWithinPartitions(new HashPartitioner(50)).persist()
//      //.repartitionAndSortWithinPartitions(new DatePartitioner(24)).persist()
//      .map { v => v._2 }
//    Predicates1.take(5).foreach(println)

    val JoindPredicates: RDD[((String, Long),(String, Long))] = predicate_1_partitioned.cartesian(predicate_2_partitioned)
    val joindPredicatesLength = JoindPredicates.count()
    println("Length after cartesian product "+ joindPredicatesLength) //1680
    JoindPredicates.take(5).foreach(println)
    println("Number of partitions "+ predicate_1_partitioned.getNumPartitions)
    //predicate_1_partitioned.take(10).foreach(println)
    println("Partitioner: "+predicate_1_partitioned.partitioner)

    var R1: RDD[((String, Long),(String, Long))] = sc.parallelize(JoindPredicates.take(joindPredicatesLength.toInt/2)).coalesce(4).persist()
    var R2: RDD[((String, Long),(String, Long))] = JoindPredicates.subtract(R1).coalesce(4).persist()
    println("lengh of R1 is "+ R1.count()+" and lenghth of R2 is "+ R2.count())

    val similarityThreshold = 0.4
    val similarityHandler = new SimilarityHandler(similarityThreshold)
    val similarPairsRdd1 = R1.map(x => (x._1._1, x._2._1, similarityHandler.jaccardPredicateSimilarityWithWordNet(x._1._1, x._2._1)))

    val similarPairsRdd2 = R2.map(x => (x._1._1, x._2._1, similarityHandler.jaccardPredicateSimilarityWithWordNet(x._1._1, x._2._1)))

    val samePredicates1 = similarPairsRdd1.filter(x => x._3 >= similarityThreshold)
    val samePredicates2 = similarPairsRdd2.filter(x => x._3 >= similarityThreshold)

    // printing similar predicates with their similarity score
    println("Predicates1 with similarity >= "+ similarityThreshold + " are: "+ samePredicates1.count())
    //prints all the similar predicates
    samePredicates1.take(10).foreach(println(_))

    println("Predicates2 with similarity >= "+ similarityThreshold + " are: "+ samePredicates2.count())
    //prints all the similar predicates
    samePredicates2.take(10).foreach(println(_))
    


    //println("Partitions structure: " +predicate_1_partitioned.glom().collect())
  }
  def predicatesDFPartitioningByKey(df1: DataFrame, df2: DataFrame) = {

    //    df1.take(10).foreach(println)
    //    val predicatesWithKeys1 = df1.map(_.getPredicate.getLocalName).distinct().zipWithIndex()
    val Predicates1 = df1.select("Predicate1").distinct()
    val Predicates2 = df2.select("Predicate2").distinct()

    println("Length of predicate 1: "+Predicates1.count())
    println("Length of predicate 2: "+Predicates2.count())

    //    //Partitioning using coalesce
    val predicate_1_partitioned = Predicates1.coalesce(5).persist()
    val predicate_2_partitioned = Predicates2.coalesce(5).persist()



    predicate_1_partitioned.foreachPartition(iterator => println("elements in this partition for Predicate 1: "+ iterator.length))
    predicate_2_partitioned.foreachPartition(iterator => println("elements in this partition for Predicate 2:"+ iterator.length))

    val JoindPredicates = predicate_1_partitioned.crossJoin(predicate_2_partitioned)
   // JoindPredicates.coalesce(5).persist().foreachPartition(iterator => println("elements in this partition for the joined predicates: "+ iterator.length))
    //JoindPredicates.take(5).foreach(println)
//    println("Number of partitions "+ predicate_1_partitioned.getNumPartitions)
    JoindPredicates.take(10).foreach(println)
//    println("Partitioner: "+predicate_1_partitioned.partitioner)


  }
//  class DomainNamePartitioner(numParts: Int, Predicates1 : RDD[(String, Long)]) extends Partitioner with Serializable {
//    override def numPartitions: Int = numParts
//    override def getPartition(key: Any): Int = {
//      //val domain = new Java.net.URL(key.toString).getHost()
//      val domain = Predicates1.keys
//      val code = (domain.hashCode % numPartitions)
//      if (code < 0) {
//        code + numPartitions // Make it non-negative
//      } else {
//        code
//      }
//    }
//    // Java equals method to let Spark compare our Partitioner objects
//    override def equals(other: Any): Boolean = other match {
//      case dnp: DomainNamePartitioner =>
//        dnp.numPartitions == numPartitions
//      case _ =>
//        false
//    }
//  }
  class DatePartitioner(partitions: Int) extends Partitioner {

    override def getPartition(key: Any): Int = {
      val start_time: Long = key.asInstanceOf[Long]
      Objects.hash(Array(start_time)) % partitions
    }

    override def numPartitions: Int = partitions
  }


}
