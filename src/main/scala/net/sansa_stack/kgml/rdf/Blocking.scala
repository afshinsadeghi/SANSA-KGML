package net.sansa_stack.kgml.rdf

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}


// Schema agnostic automated blocking
//1. First match predicates by Wordnet based PredicateMatching
//2. From last step See which of the predicates have object of type literal take those to use in step 3
//3. Then rank subject by those that use it

//to see predicates between them are mostly used, you can do multiple blocking based on getMaxCommonTypes and get ranking of the select

class Blocking(sparkSession: SparkSession) extends EvaluationHelper {

  val simThreshold = 0.7
  /*
  * get literal value in objects
   */
  val getLiteralValue = udf((S: String) => {
    //println("input:" + S)
    if (S == null) null
    else if (S.length > 0 && S.startsWith("<")) {
      try { //handling special case of Drugbank that puts casRegistryName in URIs, matching between literlas and uri
        //   var str = S.split("<")(1).split(">")(0).split("/").last
        //  if (str.endsWith(" .")) str = str.drop(2)
        // str
        null
        //println("non literal:" + S)
      } catch {
        case e: Exception => null
      }
    } else { //removing @language ending
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

  /*
 * get literal value in objects
  */
  val getComparableValue = udf((S: String) => {
    //println("input:" + S)
    if (S == null) null
    else if (S.length > 0 && S.startsWith("<")) { //for URI grab the ending for Wordnet
      try { //handling special case of Drugbank that puts casRegistryName in URIs, matching between literlas and uri
        var str = S.split("<")(1).split(">")(0).split("/").last
        //if (str.endsWith(" .")) str = str.drop(2)
        if (str.contains("#")) str = str.split("#")(1)
        //println("non literal:<" + str) //add this to recognize them in SimHandler function
        "<" + str
      } catch {
        case e: Exception => null
      }
    } else { //removing @language ending
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
    if (str.length > 0 && str.startsWith("<")) {
      try { //handling only URIs, ignoring literals
        var ending1 = str.split("<")(1).split(">")(0).split("/").last
        if (ending1.endsWith(" .")) ending1 = ending1.drop(2)
        ending1
      } catch {
        case e: Exception => null
      }
    } else {
      null
    }
  }

  /**
    * Returns a two column dataFrame of matched predicates
    *
    * @param df1
    * @param df2
    * @return
    */
  def getMatchedPredicates(df1: DataFrame, df2: DataFrame): DataFrame = {

    //1. First filter all predicates in one column dataframes A and B, I expect all fit into memory
    //2. make a cartesian comparison of all them.
    //df1.show(20, 80)
    //df2.show(20,80)
    //val dF1 = df1.select(df1("predicate1")).distinct.coalesce(5).persist()
    //  .withColumn("predicate_ending", getLastPartOfURI(col("object1")))

    //val dF2 = dF1.crossJoin(df2.select(df2("predicate2")).distinct).coalesce(5).persist()
    val dF2 = df1.select(df1("predicate1")).distinct.crossJoin(df2.select(df2("predicate2")).distinct)
    println("number of partitions after cross join = " + dF2.rdd.partitions.size) //200 partition

    //Elapsed time: 90.543716752s
    //Elapsed time: 85.588292884s without coalesce(10)
    //    .withColumn("predicate_ending", getLastPartOfURI(col("object2")))

    // val dF3 = dF2.withColumn("same_predicate", wordNetPredicateMatch(col("predicate1"), col("predicate2")))

    //dF3.createOrReplaceTempView("triple")


    //val sqlText2 = "SELECT same_predicate, COUNT(*) FROM triple group by same_predicate ORDER BY COUNT(*) DESC"
    //val predicates = sparkSession.sql(sqlText2)
    //predicates.show(15, 80)

    //println(predicates.collect().take(20))


    val wordNetSim = new SimilarityHandler(simThreshold)
    wordNetSim.setWordNetThreshold(0.55)
    println("WordNet Sim threshold for matching predicates: " + wordNetSim.getWordNetThreshold)
    val similarPairs = dF2.collect().map(x => (x.getString(0), x.getString(1),
      wordNetSim.arePredicatesEqual(getURIEnding(x.getString(0)),
        getURIEnding(x.getString(1)))))


    val rdd1 = sparkSession.sparkContext.parallelize(similarPairs)
    import sparkSession.sqlContext.implicits._
    val matched = rdd1.toDF("predicate1", "predicate2", "equal")
    //println("matched predicates:")
    //matched.show(40)
    //Elapsed time: 92.068153666s
    //Elapsed time: 103.122292326s with using cache
    //println("number of partitions for matched predicates = " + matched.rdd.partitions.size)

    matched.createOrReplaceTempView("triple1")
    val sqlText2 = "SELECT predicate1, predicate2 FROM triple1 where equal = true"
    val predicates = sparkSession.sql(sqlText2)

    //if (printReport) {
    println("Matched predictes in this step:")
    predicates.show(50, 80)

    println("Numbre of Matched predictes is :" + predicates.count())
    //}
    /*

    The result between drugdunmp dataset and dbpedia
+----------------------------------------------------------------------------+-------------------------------------------------+
|                                                                  predicate1|                                       predicate2|
+----------------------------------------------------------------------------+-------------------------------------------------+
|                           <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>|<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>|
|<http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/molecularWeight>|    <http://dbpedia.org/property/molecularWeight>|
|                                      <http://www.w3.org/2002/07/owl#sameAs>|           <http://www.w3.org/2002/07/owl#sameAs>|
|   <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/meltingPoint>|       <http://dbpedia.org/property/meltingPoint>|
|         <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/target>|             <http://dbpedia.org/property/target>|
|                                <http://www.w3.org/2000/01/rdf-schema#label>|     <http://www.w3.org/2000/01/rdf-schema#label>|
|           <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/name>|                 <http://xmlns.com/foaf/0.1/name>|
|           <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/name>|               <http://dbpedia.org/property/name>|
+----------------------------------------------------------------------------+-------------------------------------------------+

in Persons dataset:

+---------------------------------------------------------+---------------------------------------------------------+
|                                               predicate1|                                               predicate2|
+---------------------------------------------------------+---------------------------------------------------------+
|        <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>|        <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>|
|  <http://www.okkam.org/ontology_person1.owl#phone_numer>|  <http://www.okkam.org/ontology_person2.owl#phone_numer>|
|          <http://www.okkam.org/ontology_person1.owl#age>|          <http://www.okkam.org/ontology_person2.owl#age>|
|   <http://www.okkam.org/ontology_person1.owl#given_name>|   <http://www.okkam.org/ontology_person2.owl#given_name>|
|     <http://www.okkam.org/ontology_person1.owl#postcode>|     <http://www.okkam.org/ontology_person2.owl#postcode>|
|  <http://www.okkam.org/ontology_person1.owl#has_address>|  <http://www.okkam.org/ontology_person2.owl#has_address>|
|      <http://www.okkam.org/ontology_person1.owl#surname>|      <http://www.okkam.org/ontology_person2.owl#surname>|
|       <http://www.okkam.org/ontology_person1.owl#street>|       <http://www.okkam.org/ontology_person2.owl#street>|
|<http://www.okkam.org/ontology_person1.owl#date_of_birth>|<http://www.okkam.org/ontology_person2.owl#date_of_birth>|
| <http://www.okkam.org/ontology_person1.owl#house_number>| <http://www.okkam.org/ontology_person2.owl#house_number>|
|   <http://www.okkam.org/ontology_person1.owl#soc_sec_id>|   <http://www.okkam.org/ontology_person2.owl#soc_sec_id>|
+---------------------------------------------------------+---------------------------------------------------------+


           The output for exact string equality on apple DBpeida:
       +--------------+--------+
       |same_predicate|count(1)|
       +--------------+--------+
       |         false|   27636|
       |          true|       3|
       +--------------+--------+
     */

    /*

        val sqlText3 = "SELECT predicate1 FROM triple where same_predicate = true"
        val samePredicates = sparkSession.sql(sqlText3)
        samePredicates.show(15, 80)

        The output :
          <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
                   <http://www.w3.org/2002/07/owl#sameAs>
             <http://www.w3.org/2000/01/rdf-schema#label>

     */
    predicates
  }


  /**
    * Block triples based on matched predicates
    *
    * @param df1
    * @param df2
    * @param matchedPredicates
    */
  def BlockSubjectsByTypeAndLiteral(df1: DataFrame, df2: DataFrame, matchedPredicates: DataFrame): DataFrame = {

    //filter triples with literals by sparkssql
    //   val dF1 = df1.
    //    withColumn("Literal1", getLiteralValue(col("object1")))

    //  var dF2 = df2.
    //     withColumn("Literal2", getLiteralValue(col("object2")))


    var predicatesPairs = matchedPredicates.
      //  where(!col("Predicate1").contains("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>")).
      toDF("Predicate3", "Predicate4")

    df1.createOrReplaceTempView("triple")
    df2.createOrReplaceTempView("triple2")
    val samePredicateSubjectObjects = df1.
      join(predicatesPairs, df1("predicate1") <=> predicatesPairs("Predicate3")).
      join(df2, predicatesPairs("Predicate4") <=> df2("predicate2"))
    // && dF2("Literal2").isNotNull && dF1("Literal1").isNotNull

    /* or could be done like this:
     val samePredicateSubjectObjects = df1.
       join(predicatesPairs, df1("predicate1") === predicatesPairs("Predicate3"))
     val samePredicateSubject =  samePredicateSubjectObjects.
       join(df2, samePredicateSubjectObjects("Predicate4") === df2("predicate2"))
 */

    //do not compare those that just have one common predicate if we have more, to ignore the case that some subject of different type has only "type" common predicate


    //"Subject1","Predicate1","Object1","Literal1", "Predicate3","Predicate4","Subject2","Predicate2","Object2","Literal2"

    val typeSubjectWithLiteral = this.getSubjectsWithLiteral(samePredicateSubjectObjects)

    if (printReport) typeSubjectWithLiteral.show(15, 50)

    //The other way round will be comparison of subjects. but if the URI format is hashed based then that is useless.
    // By comparing filtered objects we compare literals as well.
    // We cover two cases, when ids are embedded into URI and not literals, as in case of Drug bank data set
    // There is also a chance that entities from both kgs
    // refer to same KG using sameAs link.
    typeSubjectWithLiteral.persist()
    typeSubjectWithLiteral
  }

  def getSubjectsWithLiteral(samePredicateSubjectObjects: DataFrame): DataFrame = {
    val typeSubjectWithLiteral = samePredicateSubjectObjects.withColumn("Literal1", getComparableValue(col("object1"))).
      withColumn("Literal2", getComparableValue(col("object2"))).where(col("Literal1").isNotNull && col("Literal2").isNotNull)
      .select("Subject1", "Literal1", "Subject2", "Literal2", "Predicate1", "Predicate2")
    //typeSubjectWithLiteral.show(60,40)  //here one subject and predicate can have several different literals
    typeSubjectWithLiteral
  }

  /**
    * Blocking strategy based on types: we take those subject that have the most common types in one partition
    * This function can be called when executor is set to CommonTypes
    *
    * @param df1
    * @param df2
    * @return
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


    val ranking = this.rankPredicates(df1: DataFrame, df2: DataFrame)
    ranking
  }


  /**
    *
    * @param df1
    * @param df2
    * @return
    */
  def rankPredicates(df1: DataFrame, df2: DataFrame): DataFrame = {

    df1.createOrReplaceTempView("triple")
    df2.createOrReplaceTempView("triple2")
    val dF2 = (df1.select(df1("predicate1")).distinct).crossJoin(df2.select(df2("predicate2")).distinct)


    val wordNetSim = new SimilarityHandler(simThreshold)
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
      join(predicatesPairs, df1("predicate1") <=> predicatesPairs("Predicate3")).join(df2, predicatesPairs("Predicate4") <=> df2("predicate2"))

    samePredicate.createOrReplaceTempView("sameTypes")
    println("ranking of predicates")
    val sqlText2 = "SELECT  predicate1, predicate2, COUNT(*) FROM sameTypes group by predicate1, predicate2 ORDER BY COUNT(*) DESC"
    val typedTriples2 = sparkSession.sql(sqlText2)
    if (printReport) {
      typedTriples2.show(15, 80)
    }
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

  //get parents of a matched entity and pair it with parents of its equivalent in the second data set
  def getParentEntities(df1: DataFrame, df2: DataFrame, leafSubjectsMatch: DataFrame): DataFrame = {
    //get parent nodes, removing from them those subjects the matched entities from df1 and df2 subjects
    val parentNode1 = df1.where(col("object1") === leafSubjectsMatch.col("subject1"))
      .except(df1.where(col("subject1") === leafSubjectsMatch.col("subject1")))

    val parentNode2 = df2.where(col("object2") === leafSubjectsMatch.col("subject2"))
      .except(df2.where(col("subject2") === leafSubjectsMatch.col("subject2")))

    //parentNode1.join(parentNode2, parentNode1.)
    //going one step in the neighborhood and getting pair of parents of matched entities to compare
    val parentsNodes = parentNode1.where(col("object1") === leafSubjectsMatch("subject1")).crossJoin(parentNode2.
      where(col("object2") === leafSubjectsMatch("subject2"))) //instead of cross join compare those who had same child
    if(printReport) {
      println("getting parent nodes..")
      parentsNodes.show(20, 80)
    }
    parentsNodes
  }

}
