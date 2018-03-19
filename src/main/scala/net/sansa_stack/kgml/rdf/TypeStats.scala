package net.sansa_stack.kgml.rdf

import net.sansa_stack.inference.spark.data.model.TripleUtils
import org.aksw.jena_sparql_api.utils.Triples
import org.apache.spark.rdd.RDD
import org.apache.jena.graph.Triple
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


/**
  * Created by Afshin on 22.02.18.
  */
class TypeStats(sparkSession: SparkSession) {


  /*
* get literal value in objects
 */
  val getLiteralValue = udf((S: String) => {
    //println("input:" + S)
    if (S == null) null
    else if (S.length > 0 && S.startsWith("<")) {
      try { //handling special case of Drugbank that puts casRegistryName in URIs, matching between literlas and uri
        var str = S.split("<")(1).split(">")(0).split("/").last
        if (str.endsWith(" .")) str = str.drop(2)
        str
        //println("non literal:" + S)
      } catch {
        case e: Exception => null
      }
    } else {
      //println("literal:" + S)
      if (S.length == 0) null
      else if (S.startsWith("\"")) {
        val str = S.split("\"")
        if (str == null || str.isEmpty) S
        else
          str(1)
      } else {
        S //some file does not have literals in quotation
      }
    }
  })

  def getURIEnding(str: String): String = {
    var ending1 = str.split("<")(1).split(">")(0).split("/").last
    if (ending1.endsWith(" .")) ending1 = ending1.drop(2)
    //println(ending1)
    ending1
  }

  def calculateStats(triplesRDD1: RDD[(Triple)], triplesRDD2: RDD[(Triple)]): Unit = {



    println("Getting types...")

    val predicates1 = triplesRDD1.map(_.getPredicate.getLocalName).distinct()
    println("Predicates  KG1 are " + predicates1.count())


    var trPr1 = triplesRDD1.filter(_.getPredicate.getLocalName.contentEquals("type"))

    var trSub1 = trPr1.map(_.getSubject.toString()).distinct()
    var trOb1 = trPr1.map(_.getObject.toString()).distinct()
    println("Triples defining type.. ." + trPr1.count())
    println("number of distinct objects of types.. " + trOb1.count())
    println("listing... ")
    trOb1.foreach(println)
    println("number of distinct subjects of types.. " + trSub1.count())
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
    var pairTriplesOnSubject = triplesRDD1.map(x => (x.getSubject(), x))
    //pairTriplesOnSubject.filter(_._2)

  }

  import sparkSession.implicits._

  def calculateDFStats(df1: DataFrame): Unit = {


    df1.createOrReplaceTempView("triple1")

    println("5 sample Triples of the dataset")
    val sampleTriples = sparkSession.sql("SELECT * from triple1")
    sampleTriples.show(5, 80)

    println("Number of Triples with type")
    val sqlText = "SELECT COUNT(*) FROM triple1 where predicate1 = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'"
    val typedTriples = sparkSession.sql(sqlText)
    typedTriples.show(10, 40)


    println("Distribution of objects with type")

    val typedObjectCount = sparkSession.sql("select  object1, count(*) from triple1 where predicate1" +
      " = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' group by object1 UNION ALL" +
      " SELECT 'SUM' object1, COUNT(object1) FROM triple1 where predicate1 =" +
      " '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'")
    typedObjectCount.show(40, 80)

    println("distribution of subjects with type")

    val typedSubjectCount = sparkSession.sql("select  subject1, count(*) from triple1 where predicate1 = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' group by subject1 UNION ALL SELECT 'SUM' subject1, COUNT(subject1) FROM triple1 where predicate1 = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>'")
    typedSubjectCount.show(40, 80)


    println("Number of subjects with no type")
    val notTypedSubjectCount = sparkSession.sql("Select Count(*) from ( select distinct subject1 from triple1 t1 WHERE NOT EXISTS ( SELECT 1 FROM triple1 t2  WHERE t1.subject1 = t2.subject1 AND t2.predicate1 = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>') group by subject1)")
    notTypedSubjectCount.show(40, 80)
  }


  // Blocking strategy based on types: we take those subject that have the most common types in one partition
/*
  This function can be called when executor is set to CommonTypes
 */
  def getMaxCommonTypes(df1: DataFrame, df2: DataFrame): DataFrame = {


    df1.createOrReplaceTempView("triple1")
    df2.createOrReplaceTempView("triple2")

    println("objects of Triples with type")
    val sqlText = "SELECT subject1, predicate1, object1 FROM triple1 where predicate1 = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' "
    val typedTriples = sparkSession.sql(sqlText)
    typedTriples.show(15, 80)

    println("objects of Triple2 with type")
    val sqlText2 = "SELECT  subject2, predicate2, object2 FROM triple2 where predicate2 = '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>' "
    val typedTriples2 = sparkSession.sql(sqlText2)
    typedTriples2.show(15, 80)


    println("objects of Triple1 and Triple2 with showing their types joined by type predicate")
    val samePredicate = typedTriples.join(typedTriples2, typedTriples("predicate1") <=> typedTriples2("predicate2")) //they are all types
    samePredicate.show(12, 80)


    val ranking =  this.rankPredicates(df1: DataFrame, df2: DataFrame )
  ranking
  }

  def rankPredicates(df1: DataFrame, df2: DataFrame): DataFrame ={



    df1.createOrReplaceTempView("triple")
    df2.createOrReplaceTempView("triple2")
    val dF2 = (df1.select(df1("predicate1")).distinct).crossJoin(df2.select(df2("predicate2")).distinct)


    val wordNetSim = new SimilarityHandler(0.5)
    val similarPairs = dF2.collect().map(x => (x.getString(0), x.getString(1),
      wordNetSim.arePredicatesEqual(getURIEnding(x.getString(0)),
        getURIEnding(x.getString(1)))))


    val rdd1 = sparkSession.sparkContext.parallelize(similarPairs)
    import sparkSession.sqlContext.implicits._
    var matched0 = rdd1.toDF("predicate1", "predicate2", "equal")

    matched0.createOrReplaceTempView("triple1")
    val sqlText1 = "SELECT predicate1, predicate2 FROM triple1 where equal = true"
    val matched = sparkSession.sql(sqlText1)


    val predicatesPairs = matched.toDF("Predicate3", "Predicate4")
    df1.createOrReplaceTempView("triple")
    df2.createOrReplaceTempView("triple2")
    val samePredicate = df1.
      join(predicatesPairs, df1("predicate1") <=> predicatesPairs("Predicate3")).join(df2, predicatesPairs("Predicate4") <=> df2("predicate2") )


    samePredicate.createOrReplaceTempView("sameTypes")
    println("ranking of predicates")
    val sqlText2 = "SELECT  predicate1, predicate2, COUNT(*) FROM sameTypes group by predicate1, predicate2 ORDER BY COUNT(*) DESC"
    val typedTriples2 = sparkSession.sql(sqlText2)
    typedTriples2.show(15, 80)

    /*
    For person data set

    ranking of predicates
+---------------------------------------------------------+---------------------------------------------------------+--------+
|                                               predicate1|                                               predicate2|count(1)|
+---------------------------------------------------------+---------------------------------------------------------+--------+
|        <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>|        <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>| 2000000|
| <http://www.okkam.org/ontology_person1.owl#house_number>| <http://www.okkam.org/ontology_person2.owl#house_number>|  250000|
|  <http://www.okkam.org/ontology_person1.owl#phone_numer>|  <http://www.okkam.org/ontology_person2.owl#phone_numer>|  250000|
|          <http://www.okkam.org/ontology_person1.owl#age>|          <http://www.okkam.org/ontology_person2.owl#age>|  250000|
|   <http://www.okkam.org/ontology_person1.owl#soc_sec_id>|   <http://www.okkam.org/ontology_person2.owl#soc_sec_id>|  250000|
|  <http://www.okkam.org/ontology_person1.owl#has_address>|  <http://www.okkam.org/ontology_person2.owl#has_address>|  250000|
|      <http://www.okkam.org/ontology_person1.owl#surname>|      <http://www.okkam.org/ontology_person2.owl#surname>|  250000|
|       <http://www.okkam.org/ontology_person1.owl#street>|       <http://www.okkam.org/ontology_person2.owl#street>|  250000|
|   <http://www.okkam.org/ontology_person1.owl#given_name>|   <http://www.okkam.org/ontology_person2.owl#given_name>|  250000|
|     <http://www.okkam.org/ontology_person1.owl#postcode>|     <http://www.okkam.org/ontology_person2.owl#postcode>|  250000|
|<http://www.okkam.org/ontology_person1.owl#date_of_birth>|<http://www.okkam.org/ontology_person2.owl#date_of_birth>|  250000|
+---------------------------------------------------------+---------------------------------------------------------+--------+


     */

    typedTriples2
  }

  def rankDFSubjectsByType(df1: DataFrame, df2: DataFrame): Unit = {

    val dF1 = df1.
      withColumn("Literal1", getLiteralValue(col("object1")))

    val dF2 = df2.
      withColumn("Literal2", getLiteralValue(col("object2")))

    dF1.createOrReplaceTempView("triple")
    dF2.createOrReplaceTempView("triple2")
     val samePredicateAndObject = dF1.join(dF2, dF1("predicate1") <=> dF2("predicate2")
       && dF1("literal1") <=> dF2("literal2"))

    samePredicateAndObject.createOrReplaceTempView("sameTypes")
    println("ranking of subjects based on common type.(I used common predicate and objects which is more general than common type)")
    val sqlText2 = "SELECT  subject1, subject2, COUNT(*) FROM sameTypes group by subject1,subject2 ORDER BY COUNT(*) DESC"
    val typedTriples2 = sparkSession.sql(sqlText2)
    typedTriples2.show(15, 80)


  }

  /*
  * giving number of same as links in a data set
   */
  def countSameASLinks(df1:DataFrame): Unit ={
    var df2 = df1.toDF("Subject1", "Predicate1", "Object1")
    df2.createOrReplaceTempView("triple1")
    println("Counting number of same as links in the KG:")

    val typedSubjectCount = sparkSession.sql("select predicate1, count(*) from triple1 where predicate1 = '<http://www.w3.org/2002/07/owl#sameAs>' group by predicate1")
    typedSubjectCount.show(20, 60)

    /*
    In drugbank dataset the result was
    Counting number of same as links in the KG:
+--------------------------------------+--------+
|                            predicate1|count(1)|
+--------------------------------------+--------+
|<http://www.w3.org/2002/07/owl#sameAs>|    9521|
+--------------------------------------+--------+
     */

  }
}
