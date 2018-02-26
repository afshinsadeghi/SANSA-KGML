package net.sansa_stack.kgml.rdf

import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple

/**
  * Created by Afshin on 22.02.18.
  */
class TypeStats {

  def calculateStats(triplesRDD1 : RDD[(Triple)], triplesRDD2 : RDD[(Triple)]) : Unit = {


    println("Getting types...")

    val predicates1 = triplesRDD1.map(_.getPredicate.getLocalName).distinct()
    println("Predicates  KG1 are " + predicates1.count())



    var trPr1 = triplesRDD1.filter(_.getPredicate.getLocalName.contentEquals("type"))

    var trSub1 = trPr1.map(_.getSubject.toString()).distinct()
    var trOb1 = trPr1.map(_.getObject.toString()).distinct()
    println("Triples defining type.. ." + trPr1.count())
    println("number of distinct objects of types.. "+ trOb1.count())
    println("listing... ")
    trOb1.foreach(println)
    println("number of distinct subjects of types.. "+ trSub1.count())

//finding those subjects that has no type
    var pairTriplesOnSubject = triplesRDD1.map(x => (x.getSubject(), x ))
    pairTriplesOnSubject.filter(_._2)

  }


}
