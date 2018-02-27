package net.sansa_stack.kgml.rdf

import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
/*
* Created by Shimaa
*
* */

class Partitioning {
  def predicatesPartitioningByKey(Predicates1 : RDD[(String, Long)], Predicates2 : RDD[(String, Long)]) = {


    Predicates1.take(5).foreach(println)

    val tunedPartitioner = new RangePartitioner(8, Predicates1.union(Predicates2))
    println("tunedPartitioner "+ tunedPartitioner + tunedPartitioner.toString+ tunedPartitioner.numPartitions)

    val partitioned = Predicates1.union(Predicates2).partitionBy(tunedPartitioner).persist()
    println("Number of partitions"+ partitioned.getNumPartitions)
    partitioned.take(10).foreach(println)
    



  }


}
