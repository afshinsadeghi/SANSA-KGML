package net.sansa_stack.kgml.rdf

import net.sansa_stack.inference.spark.data.model.TripleUtils
import org.aksw.jena_sparql_api.utils.Triples
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Afshin on 22.02.18.
  */
class TypeStats(sparkSession : SparkSession) {

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
    //println("listing... ")
    //trSub1.foreach(println)

    // Solution:
    // I make a unique (distinct)RDD of subjects then for all them filter those who have types.      -> not possible, making distinc n subjects removes theior relation to types.unlss we have a graphx.



    //triplesRDD1. ( TripleUtils)
     // .toDF()



   // then from those who have type are clear.
   // then all subject minus filtered subject become no type subjects(substract).
   // Then find the type of those who have types.
   // filter those triples with type then for each object count the number of subjects.

   // This solution is costly, I must pass all triples to make a unique list of subjects.
   // so I start with predicates. Filter all triples with type predicates. Then get their subject. to get those subjects
    // with no type, remove all occurrence of subjects of those with types from all triples.

    //finding those subjects that has no type
    var pairTriplesOnSubject = triplesRDD1.map(x => (x.getSubject(), x ))
    //pairTriplesOnSubject.filter(_._2)

  }


  def calculateDFStats(df1 : DataFrame): Unit = {


    import sparkSession.implicits._
    df1.createOrReplaceTempView( "triple" )

    println("5 sample Triples of the dataset")
    val   sampleTriples  =  sparkSession.sql("SELECT * from triple")
    sampleTriples.show(5, 40)

    println("Number of Triples with type")
    val  sqlText  =  "SELECT COUNT(*) FROM triple where predicate = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'"
    val   typedTriples  =  sparkSession.sql(sqlText)
    typedTriples.show(10, 40)


    println("distribution of objects with type")

    val typedObjectCount  = sparkSession.sql( "select  object, count(*) from triple where predicate = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' group by object UNION ALL SELECT 'SUM' object, COUNT(object) FROM triple where predicate = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'" )
    typedObjectCount.show(40, 40)

    println("distribution of subjects with type")

    val typedSubjectCount = sparkSession.sql( "select  subject, count(*) from triple where predicate = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' group by subject UNION ALL SELECT 'SUM' subject, COUNT(subject) FROM triple where predicate = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'" )
    typedSubjectCount.show(40, 40)


   // println("Number of subjects with no type")
   // val notTypedSubjectCount = sparkSession.sql( "select subject, count( DISTINCT subject) from triple t1 WHERE NOT EXISTS ( SELECT 1 FROM triple t2  WHERE t1.subject = t2.subject AND t2.predicate = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>') group by subject UNION ALL SELECT * " )
   // notTypedSubjectCount.show(40, 60)
  }
}
