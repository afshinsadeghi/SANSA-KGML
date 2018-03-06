package net.sansa_stack.kgml.rdf

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
/*
* Created by Shimaa
*
* */

class Partitioning {
  /*
  * This class takes two rdd for prdicates (one rdd for each dataset) and partitioned each rdd separtely, then apply cartesian product on the partitioned rdd
  * */
  def predicatesPartitioningByKey(Predicates1 : RDD[(String, Long)], Predicates2 : RDD[(String, Long)]) = {


    //Predicates1.take(5).foreach(println)

    //val tunedPartitioner = new RangePartitioner(8, Predicates1)
    //println("tunedPartitioner "+ tunedPartitioner + tunedPartitioner.toString+ tunedPartitioner.numPartitions)
    //val predicate_1_partitioned = Predicates1.partitionBy(tunedPartitioner).persist()
    //val tunedPartitioner = new DomainNamePartitioner(4, Predicates1)
    //val predicate_1_partitioned = Predicates1.partitionBy(new DomainNamePartitioner(4, Predicates1)).persist()



    val predicate_1_partitioned = Predicates1.partitionBy(new RangePartitioner(4, Predicates1)).persist()
    //val predicate_1_partitioned = Predicates1.partitionBy(new HashPartitioner(10)).persist()
    val predicate_2_partitioned = Predicates2.partitionBy(new RangePartitioner(4, Predicates2)).persist()
    //val predicate_2_partitioned = Predicates2.partitionBy(new HashPartitioner(10)).persist()
    val JoindPredicates = predicate_1_partitioned.cartesian(predicate_2_partitioned)
    JoindPredicates.take(5).foreach(println)
    println("Number of partitions "+ predicate_1_partitioned.getNumPartitions)
    //predicate_1_partitioned.take(10).foreach(println)
    println("Partitioner: "+predicate_1_partitioned.partitioner)

    //println("Partitions structure: " +predicate_1_partitioned.glom().collect())
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

}
